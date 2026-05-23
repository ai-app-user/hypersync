#include "jobs/transfer_engine/transfer_engine.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cctype>
#include <ctime>
#include <cstring>
#include <cmath>
#include <deque>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <map>
#include <optional>
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
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/wait.h>
#include <unistd.h>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

#include "common/buffer_pool.hpp"
#include "common/config.hpp"
#include "common/filesystem_utils.hpp"
#include "common/fixed_string.hpp"
#include "common/hash_utils.hpp"
#include "core/diff_result_buffer_codec.hpp"
#include "core/flat_folder_buffer_codec.hpp"
#include "core/metadata_record_writer.hpp"
#include "core/metadata_buffer_codec.hpp"
#include "core/nfs_backend.hpp"
#include "common/path_utils.hpp"
#include "common/preallocated_ring.hpp"
#include "common/records.hpp"
#include "common/socket_utils.hpp"
#include "common/slot_pool.hpp"
#include "common/state_machine.hpp"
#include "common/version.hpp"
#include "common/watermarks.hpp"
#include "utils/content_hash.hpp"
#include "core/data_buffer_codec.hpp"
#include "core/pipeline_buffers.hpp"
#include "jobs/buffer_discarder/buffer_discarder.hpp"
#include "jobs/buffer_generator/buffer_generator.hpp"
#include "jobs/buffer_transport/buffer_transport.hpp"
#include "jobs/checker/checker.hpp"
#include "jobs/data_cacher/data_cacher.hpp"
#include "jobs/data_hasher/data_hasher.hpp"
#include "jobs/data_writer/data_writer.hpp"
#include "jobs/file_metadata_generator/file_metadata_generator.hpp"
#include "jobs/metadata_stats_discarder/metadata_stats_discarder.hpp"
#include "jobs/metadata_record_writer_job/metadata_record_writer_job.hpp"
#include "jobs/nfs_data_reader/nfs_data_buffer_reader.hpp"
#include "jobs/nfs_data_reader/nfs_data_reader.hpp"
#include "jobs/nfs_meta_reader/nfs_meta_reader.hpp"
#include "monitoring/autoscaler.hpp"
#include "monitoring/status_monitor.hpp"

namespace hypersync {

namespace {

inline constexpr std::size_t kMetadataPartitionTransportPoolSlots = 1024U;
inline constexpr std::size_t kMetadataDiscardDefaultBufferSlots = 128U;

void pin_copy_target_classifier_thread(std::size_t lane_index) noexcept {
#if defined(__linux__)
    const unsigned int hardware_cpus = std::thread::hardware_concurrency();
    if (hardware_cpus == 0U) {
        return;
    }
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(static_cast<int>((8U + (lane_index % 8U)) % hardware_cpus), &set);
    (void)pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
#else
    (void)lane_index;
#endif
}

std::string nfs_url_with_server_expression(std::string_view root_url, std::string_view server_expression) {
    if (server_expression.empty()) {
        return std::string(root_url);
    }
    constexpr std::string_view kPrefix = "nfs://";
    if (root_url.rfind(kPrefix, 0) != 0) {
        throw std::runtime_error("VIP target partitioning requires an nfs:// target URL");
    }
    const std::size_t server_begin = kPrefix.size();
    const std::size_t server_end = root_url.find('/', server_begin);
    if (server_end == std::string_view::npos || server_end == server_begin) {
        throw std::runtime_error("invalid nfs:// target URL for VIP target partitioning");
    }

    std::string url;
    url.reserve(kPrefix.size() + server_expression.size() + (root_url.size() - server_end));
    url.append(kPrefix);
    url.append(server_expression);
    url.append(root_url.substr(server_end));
    return url;
}

FileSpec file_spec_from_data_trailer(const DataBufTrailer& trailer) {
    FileSpec file;
    file.rel_path = std::string(trailer.rel_path.view());
    file.declared_size = trailer.file_size;
    file.mtime = trailer.mtime;
    file.mode = trailer.mode != 0U ? trailer.mode : 0644U;
    file.uid = trailer.uid;
    file.gid = trailer.gid;
    return file;
}

enum class PriorityMessageType : std::uint32_t {
    session_start = 1,
    file_record = 2,
    file_decision = 3,
    file_ack = 4,
    session_end = 5,
    pause = 6,
    resume = 7,
    directory_record = 8,
};

struct FileRecordMessage {
    std::uint64_t file_id = 0;
    std::uint64_t folder_hash = 0;
    FixedString<kDataBufRelPathBytes> rel_path;
    std::uint64_t size = 0;
    std::uint64_t mtime = 0;
    std::uint32_t mode = 0;
    std::uint32_t uid = 0;
    std::uint32_t gid = 0;
};

struct FileDecisionMessage {
    std::uint64_t file_id = 0;
    bool skip = false;
};

struct DirectoryRecordMessage {
    FixedString<kDataBufRelPathBytes> rel_path;
    std::uint64_t mtime = 0;
    std::uint32_t mode = 0;
    std::uint32_t uid = 0;
    std::uint32_t gid = 0;
};

struct FileAckMessage {
    std::uint64_t file_id = 0;
    bool hash_verified = false;
    std::uint64_t bytes_written = 0;
};

struct ReceiverFileContext {
    FileRecordMessage record;
    std::uint64_t bytes_written = 0;
    std::uint64_t next_pause_after_bytes = 0;
};

struct ReceiverSharedState {
    ReceiverRuntimeConfig runtime;
    bool skip_verify = false;
    bool session_done = false;
    std::uint64_t session_large_chunk_bytes = kLargeChunkBytes;
    std::size_t small_pool_slots = 0;
    std::size_t large_pool_slots = 0;
    int priority_fd = -1;
    int data_fd = -1;
    std::mutex metadata_mutex;
    std::condition_variable metadata_cv;
    std::mutex priority_write_mutex;
    std::unordered_map<std::uint64_t, FileRecordMessage> pending_records;
    std::vector<FileSpec> directory_specs;
};

struct CachedFilePlan {
    std::uint64_t data_hash = 0;
    std::size_t chunk_count = 0;
    std::vector<std::uint64_t> entry_ids;
};

struct PreparedTransfer {
    FileSpec file;
    FileRecordMessage record;
    std::uint64_t data_hash = 0;
    std::size_t chunk_count = 0;
    bool content_loaded = false;
    bool cached = false;
    std::filesystem::path source_path;
    std::vector<std::uint64_t> cached_entry_ids;
};

struct PendingTransferFile {
    FileSpec file;
    FileRecordMessage record;
};

struct AwaitingAckTransfer {
    PreparedTransfer prepared;
    std::uint64_t data_hash = 0;
    std::size_t chunk_count = 0;
};

struct PreparedTransferQueue {
    std::mutex mutex;
    std::condition_variable cv_not_empty;
    std::condition_variable cv_not_full;
    PreallocatedRing<PreparedTransfer> queue;
    std::size_t max_entries = 1;
    bool input_done = false;
    bool stop = false;
    std::exception_ptr producer_error;
};

struct ReceiverQueuedChunk {
    DataSlotHandle handle;
    FileRecordMessage record;
};

struct ReceiverChunkQueue {
    std::mutex mutex;
    std::condition_variable cv_not_empty;
    std::condition_variable cv_not_full;
    PreallocatedRing<ReceiverQueuedChunk> queue;
    std::uint64_t capacity_bytes = 0;
    std::uint64_t queued_bytes = 0;
    bool paused = false;
    bool input_done = false;
    std::exception_ptr worker_error;
};

struct FlatMetadataWorkQueue {
    std::mutex mutex;
    std::condition_variable cv;
    std::deque<FileSpec> folders;
    std::optional<std::chrono::steady_clock::time_point> stop_at;
    std::size_t active = 0;
    bool done = false;
    std::exception_ptr error;
};

struct DiffTargetFolderQueue {
    std::mutex mutex;
    std::condition_variable cv;
    std::deque<FileSpec> folders;
    std::size_t active = 0;
    std::size_t max_entries = 65536;
    bool input_done = false;
    bool done = false;
    std::exception_ptr error;
};

struct DiffBatchQueue {
    std::mutex mutex;
    std::condition_variable cv_not_empty;
    std::condition_variable cv_not_full;
    std::deque<FlatFolderScanBatch> batches;
    std::size_t max_entries = 65536;
    std::size_t producers_remaining = 0;
    bool done = false;
    std::exception_ptr error;
};

struct FakeRemoteProcessorQueue {
    std::mutex mutex;
    std::condition_variable cv_not_empty;
    std::condition_variable cv_not_full;
    std::deque<FileSpec> folders;
    std::size_t max_entries = 65536;
    std::size_t producers_remaining = 0;
    bool done = false;
    std::exception_ptr error;
};

struct DiffPipelineTimingCounters {
    std::atomic<std::uint64_t> source_wait_target_queue_ns{0};
    std::atomic<std::uint64_t> source_wait_batch_queue_ns{0};
    std::atomic<std::uint64_t> fake_remote_wait_request_ns{0};
    std::atomic<std::uint64_t> fake_remote_wait_processor_queue_ns{0};
    std::atomic<std::uint64_t> fake_remote_delay_ns{0};
    std::atomic<std::uint64_t> fake_remote_wait_batch_queue_ns{0};
    std::atomic<std::uint64_t> joiner_idle_ns{0};
    std::atomic<std::uint64_t> joiner_process_ns{0};
};

using DiffBatchQueueShards = std::vector<std::unique_ptr<DiffBatchQueue>>;

struct DataReadFileQueue {
    std::mutex mutex;
    std::condition_variable cv_not_empty;
    std::condition_variable cv_not_full;
    std::deque<FileSpec> files;
    std::optional<std::chrono::steady_clock::time_point> stop_at;
    std::size_t max_entries = 1024;
    std::size_t resume_entries = 0;
    bool wait_for_low_watermark = false;
    bool input_done = false;
    bool stop = false;
    std::exception_ptr error;
};

struct FolderReadyFileBatch {
    FileSpec folder;
    std::vector<FileSpec> directories;
    std::vector<FileSpec> files;
    std::uint64_t logical_size_bytes = 0;
};

struct FolderReadyBatchQueue {
    std::mutex mutex;
    std::condition_variable cv_not_empty;
    std::condition_variable cv_not_full;
    std::deque<FolderReadyFileBatch> batches;
    std::optional<std::chrono::steady_clock::time_point> stop_at;
    std::size_t max_entries = 4096;
    std::size_t high_watermark = 0;
    bool input_done = false;
    bool stop = false;
    std::exception_ptr error;
};

struct ReadyFileSpillway {
    std::mutex mutex;
    std::condition_variable cv_not_empty;
    std::deque<FileSpec> files;
    bool input_done = false;
    bool stop = false;
    std::exception_ptr error;
    std::size_t high_watermark = 0;
};

std::size_t queued_ready_file_spillway(ReadyFileSpillway& spillway);

struct TargetBufferSpillway {
    std::mutex mutex;
    std::condition_variable cv_not_empty;
    std::deque<BufferHandle> handles;
    bool input_done = false;
    bool stop = false;
    std::exception_ptr error;
    std::size_t high_watermark = 0;
    std::atomic<std::int64_t>* depth_counter = nullptr;
};

struct ScannerCapacityControl {
    std::atomic<std::size_t> active_workers {1};
    std::mutex mutex;
    std::condition_variable cv;
};

struct SplitDataReadFileQueues {
    DataReadFileQueue small;
    DataReadFileQueue large;
    std::uint64_t small_file_threshold = 128U * 1024U;
};

enum class SplitDataReadRoute {
    Small,
    Large,
};

thread_local SplitDataReadRoute current_split_data_read_route = SplitDataReadRoute::Large;

struct SplitDataReadAutoscaleConfig {
    bool pipeline_enabled = false;
    bool large_enabled = false;
    std::size_t large_initial_workers = 0;
    std::size_t large_min_workers = 1;
    std::uint64_t interval_ms = 1000;
    std::string profile_name;
    std::filesystem::path settings_path;
};

struct ReconScanStats {
    std::atomic<std::size_t> files_found {0};
    std::atomic<std::size_t> folders_found {0};
    std::atomic<std::size_t> small_files_found {0};
    std::atomic<std::size_t> large_files_found {0};
    std::atomic<std::uint64_t> logical_size_bytes {0};
    std::atomic<std::uint64_t> small_logical_size_bytes {0};
    std::atomic<std::uint64_t> large_logical_size_bytes {0};
    std::atomic<bool> completed {false};
};

std::filesystem::path default_autoscale_settings_path() {
    const char* env_path = std::getenv("HYPERSYNC_AUTOSCALE_SETTINGS");
    if (env_path != nullptr && *env_path != '\0') {
        return std::filesystem::path(env_path);
    }
    const char* home = std::getenv("HOME");
    if (home != nullptr && *home != '\0') {
        return std::filesystem::path(home) / ".config" / "hypersync" / "autoscale.yaml";
    }
    return std::filesystem::path("hypersync-autoscale.yaml");
}

SplitBucketPriorityDecision choose_split_bucket_priority_workers_impl(
    const SplitBucketPriorityInput& input) noexcept {
    const std::size_t max_small = std::max<std::size_t>(1, input.max_small_workers);
    const std::size_t max_large = std::max<std::size_t>(1, input.max_large_workers);
    const std::size_t total_workers = std::max<std::size_t>(
        2,
        std::max<std::size_t>(1, input.current_small_workers) +
            std::max<std::size_t>(1, input.current_large_workers));
    const std::uint64_t small_remaining =
        input.small_total > input.small_done ? input.small_total - input.small_done : 0U;
    const std::uint64_t large_remaining =
        input.large_total_bytes > input.large_done_bytes
            ? input.large_total_bytes - input.large_done_bytes
            : 0U;

    auto eta = [](std::uint64_t remaining, double rate) {
        if (remaining == 0U) {
            return 0.0;
        }
        return rate > 0.0 ? static_cast<double>(remaining) / rate
                          : std::numeric_limits<double>::infinity();
    };

    SplitBucketPriorityDecision decision;
    decision.small_eta_seconds = eta(small_remaining, input.small_files_per_second);
    decision.large_eta_seconds = eta(large_remaining, input.large_bytes_per_second);
    if (small_remaining == 0U && large_remaining == 0U) {
        decision.small_workers = std::min(max_small, std::max<std::size_t>(1, input.current_small_workers));
        decision.large_workers = std::min(max_large, std::max<std::size_t>(1, input.current_large_workers));
        return decision;
    }
    if (large_remaining == 0U) {
        decision.small_workers = std::min(max_small, std::max<std::size_t>(1, total_workers - 1U));
        decision.large_workers = 1U;
        decision.large_reader_small_priority_percent = 100U;
        return decision;
    }
    if (small_remaining == 0U) {
        decision.small_workers = 1U;
        decision.large_workers = std::min(max_large, std::max<std::size_t>(1, total_workers - 1U));
        return decision;
    }

    const double small_per_worker =
        input.small_files_per_second > 0.0
            ? input.small_files_per_second / static_cast<double>(std::max<std::size_t>(1, input.current_small_workers))
            : 1.0;
    const double large_per_worker =
        input.large_bytes_per_second > 0.0
            ? input.large_bytes_per_second / static_cast<double>(std::max<std::size_t>(1, input.current_large_workers))
            : 1.0;
    const double small_demand = static_cast<double>(small_remaining) / std::max(1.0, small_per_worker);
    const double large_demand = static_cast<double>(large_remaining) / std::max(1.0, large_per_worker);
    const double total_demand = small_demand + large_demand;
    std::size_t small_workers =
        total_demand > 0.0
            ? static_cast<std::size_t>(std::llround(static_cast<double>(total_workers) *
                                                    small_demand / total_demand))
            : std::max<std::size_t>(1, input.current_small_workers);
    small_workers = std::clamp<std::size_t>(small_workers, 1U, max_small);
    std::size_t large_workers = total_workers > small_workers ? total_workers - small_workers : 1U;
    if (large_workers > max_large) {
        large_workers = max_large;
        small_workers = std::min(max_small, std::max<std::size_t>(1, total_workers - large_workers));
    }
    if (small_workers > max_small) {
        small_workers = max_small;
        large_workers = std::min(max_large, std::max<std::size_t>(1, total_workers - small_workers));
    }
    decision.small_workers = std::max<std::size_t>(1, small_workers);
    decision.large_workers = std::max<std::size_t>(1, large_workers);
    if (small_remaining != 0U &&
        std::isfinite(decision.small_eta_seconds) &&
        std::isfinite(decision.large_eta_seconds) &&
        decision.small_eta_seconds > decision.large_eta_seconds * 1.10) {
        const double imbalance =
            (decision.small_eta_seconds - decision.large_eta_seconds) /
            std::max(1.0, decision.small_eta_seconds);
        decision.large_reader_small_priority_percent =
            static_cast<std::uint32_t>(
                std::clamp<std::size_t>(
                    static_cast<std::size_t>(std::llround(imbalance * 100.0)),
                    0U,
                    100U));
    }
    return decision;
}

BucketPathOverloadScores evaluate_bucket_path_overload_impl(
    const BucketPathOverloadInput& input) noexcept {
    BucketPathOverloadScores scores;

    if (std::isfinite(input.small_eta_seconds) &&
        std::isfinite(input.large_eta_seconds) &&
        input.small_eta_seconds > 0.0 &&
        input.large_eta_seconds > 0.0) {
        const double eta_ratio = input.small_eta_seconds / input.large_eta_seconds;
        if (eta_ratio > 1.10) {
            scores.small_score = std::max(scores.small_score, eta_ratio / 1.10);
        }
        if (eta_ratio < 0.90) {
            scores.small_score = std::min(scores.small_score, std::max(0.25, eta_ratio / 0.90));
        }
    }

    if (input.small_low_watermark_files > 0U &&
        input.queued_small_files < input.small_low_watermark_files &&
        input.small_low_watermark_intervals >= 3U) {
        const double depletion =
            1.0 +
            static_cast<double>(input.small_low_watermark_files - input.queued_small_files) /
                static_cast<double>(input.small_low_watermark_files);
        scores.small_score = std::max(scores.small_score, depletion);
    }

    if (input.small_high_watermark_files > 0U &&
        input.queued_small_files >= input.small_high_watermark_files &&
        input.small_scanner_sleep_ratio > 0.50) {
        scores.small_score = std::min(scores.small_score, 0.75);
    }

    if (input.large_queue_capacity_files > 0U &&
        input.queued_large_files >= input.large_queue_capacity_files &&
        input.total_gigabits_per_second > 0.0 &&
        input.total_gigabits_per_second < input.large_overload_floor_gigabits_per_second) {
        const double baseline = std::max(1.0, input.line_rate_gigabits_per_second);
        scores.large_score = std::max(scores.large_score,
                                      baseline / std::max(1.0, input.total_gigabits_per_second));
    }

    if (input.large_queue_capacity_files > 0U &&
        input.queued_large_files < input.large_queue_capacity_files / 5U &&
        input.large_gigabits_per_second >= input.line_rate_gigabits_per_second * 0.80) {
        scores.large_score = std::min(scores.large_score, 0.80);
    }

    scores.small_score = std::clamp(scores.small_score, 0.25, 4.0);
    scores.large_score = std::clamp(scores.large_score, 0.25, 4.0);
    return scores;
}

std::string metadata_part_suffix(std::size_t index) {
    std::ostringstream out;
    out << std::setw(5) << std::setfill('0') << index;
    return out.str();
}

std::string metadata_extension_for_format(MetadataRecordFormat format) {
    switch (format) {
        case MetadataRecordFormat::text:
            return "txt";
        case MetadataRecordFormat::csv:
            return "csv";
        case MetadataRecordFormat::parquet:
            return "parquet";
    }
    return "txt";
}

std::filesystem::path metadata_partition_output_path(const std::filesystem::path& output_path,
                                                     MetadataRecordFormat format,
                                                     std::size_t partitions,
                                                     std::size_t index) {
    if (partitions == 1U) {
        return output_path;
    }
    return output_path / ("part-" + metadata_part_suffix(index) + "." + metadata_extension_for_format(format));
}

std::filesystem::path metadata_partition_socket_path(const std::filesystem::path& output_path, std::size_t index) {
    const std::filesystem::path candidate = output_path / ("part-" + metadata_part_suffix(index) + ".sock");
    if (candidate.string().size() <= 90U) {
        return candidate;
    }
    std::ostringstream name;
    name << "hypersync-" << std::hex << hash64(output_path.string()) << '-'
         << metadata_part_suffix(index) << ".sock";
    return std::filesystem::path("/tmp") / name.str();
}

std::filesystem::path metadata_partition_report_path(const std::filesystem::path& output_path, std::size_t index) {
    return output_path / ("part-" + metadata_part_suffix(index) + ".report");
}

std::size_t metadata_partition_for_path(std::string_view path, std::size_t partitions) {
    return static_cast<std::size_t>(hash64(path) % std::max<std::size_t>(1U, partitions));
}

std::size_t metadata_partition_for_flat_folder_buffer(const FlatFolderBufferInfo& info,
                                                      std::size_t partitions) {
    const std::size_t safe_partitions = std::max<std::size_t>(1U, partitions);
    std::uint64_t key = hash64(info.folder_path);
    key ^= (static_cast<std::uint64_t>(info.sequence) + 0x9e3779b97f4a7c15ULL + (key << 6U) + (key >> 2U));
    key ^= (info.metadata_hash + 0xbf58476d1ce4e5b9ULL + (key << 6U) + (key >> 2U));
    key ^= (static_cast<std::uint64_t>(info.child_record_count) + 0x94d049bb133111ebULL +
            (key << 6U) + (key >> 2U));
    return static_cast<std::size_t>(key % safe_partitions);
}

std::size_t metadata_transport_payload_bytes(RawBufferPool& pool, const BufferHandle& handle) {
    if (handle.pool_id == kMetadataBufferPoolId) {
        const auto& buffer = metadata_buffer(pool, handle);
        return offsetof(MetadataBuffer, bytes) + buffer.bytes_used;
    }
    if (handle.pool_id == kMetadataBatchBufferPoolId) {
        const auto& buffer = metadata_batch_buffer(pool, handle);
        return offsetof(MetadataBatchBuffer, bytes) + buffer.bytes_used;
    }
    return pool.buffer_size_bytes();
}

void write_metadata_partition_report(const std::filesystem::path& path,
                                     std::uint64_t files_written,
                                     std::uint64_t folders_written) {
    std::ofstream output(path, std::ios::out | std::ios::trunc);
    if (!output) {
        throw std::runtime_error("failed to open metadata partition report: " + path.string());
    }
    output << files_written << ' ' << folders_written << '\n';
}

std::pair<std::uint64_t, std::uint64_t> read_metadata_partition_report(const std::filesystem::path& path) {
    std::ifstream input(path);
    if (!input) {
        throw std::runtime_error("failed to read metadata partition report: " + path.string());
    }
    std::uint64_t files_written = 0;
    std::uint64_t folders_written = 0;
    input >> files_written >> folders_written;
    if (!input) {
        throw std::runtime_error("malformed metadata partition report: " + path.string());
    }
    return {files_written, folders_written};
}

void run_metadata_writer_partition_process(const MetadataRecordWriterConfig& writer_config,
                                           const std::filesystem::path& output_path,
                                           std::size_t partitions,
                                           std::size_t partition_index) {
    MetadataRecordWriterConfig partition_config = writer_config;
    partition_config.output_path =
        metadata_partition_output_path(output_path, writer_config.format, partitions, partition_index);
    if (partition_config.run_info.has_value()) {
        partition_config.run_info->run_id += "-part-" + metadata_part_suffix(partition_index);
        partition_config.run_info->settings_json +=
            ",{\"partition\":" + std::to_string(partition_index) +
            ",\"partitions\":" + std::to_string(partitions) + "}";
    }

    RawBufferPool receive_pool(kMetadataBatchBufferPoolId,
                               kMetadataPartitionTransportPoolSlots,
                               sizeof(MetadataBatchBuffer),
                               alignof(MetadataBatchBuffer));
    BufferPoolRegistry receive_registry;
    receive_registry.register_pool(receive_pool);
    BufQueue writer_queue(receive_pool.capacity());
    BufferReceiverJob receiver(1U,
                               receive_pool,
                               writer_queue,
                               BufferTransportEndpoint::unix_socket(metadata_partition_socket_path(output_path, partition_index)));
    MetadataRecordWriterJob writer(1U, writer_queue, receive_registry, partition_config);

    receiver.start();
    writer.start();
    receiver.wait();
    writer.wait();

    const auto stats = writer.writer_stats();
    write_metadata_partition_report(metadata_partition_report_path(output_path, partition_index),
                                    stats.files_written,
                                    stats.folders_written);
}

void run_metadata_discard_partition_process(const std::filesystem::path& output_path,
                                            std::size_t partition_index) {
    RawBufferPool receive_pool(kMetadataBatchBufferPoolId,
                               kMetadataPartitionTransportPoolSlots,
                               sizeof(MetadataBatchBuffer),
                               alignof(MetadataBatchBuffer));
    BufferPoolRegistry receive_registry;
    receive_registry.register_pool(receive_pool);
    BufQueue discard_queue(receive_pool.capacity());
    BufferReceiverJob receiver(1U,
                               receive_pool,
                               discard_queue,
                               BufferTransportEndpoint::unix_socket(metadata_partition_socket_path(output_path, partition_index)));
    BufferDiscarderJob discarder(BufferDiscarderConfig(1U), discard_queue, receive_registry);

    receiver.start();
    discarder.start();
    receiver.wait();
    discarder.wait();

    write_metadata_partition_report(metadata_partition_report_path(output_path, partition_index), 0U, 0U);
}

class PartitionedMetadataWriter {
public:
    PartitionedMetadataWriter(MetadataRecordWriterConfig writer_config,
                              std::filesystem::path output_path,
                              std::size_t partitions,
                              RawBufferPool* routed_source_pool = nullptr,
                              bool discard_partitions = false)
        : writer_config_(std::move(writer_config)),
          output_path_(std::move(output_path)),
          partitions_(std::max<std::size_t>(1U, partitions)),
          routed_source_pool_(routed_source_pool),
          discard_partitions_(discard_partitions) {
        if (partitions_ <= 1U) {
            throw std::invalid_argument("partitioned metadata writer requires more than one partition");
        }
        std::filesystem::create_directories(output_path_);
        children_.reserve(partitions_);
        states_.reserve(partitions_);
        route_states_.reserve(partitions_);

        for (std::size_t index = 0; index < partitions_; ++index) {
            std::filesystem::remove(metadata_partition_socket_path(output_path_, index));
            std::filesystem::remove(metadata_partition_report_path(output_path_, index));
            const pid_t child = ::fork();
            if (child < 0) {
                throw std::runtime_error("failed to fork metadata writer partition process");
            }
            if (child == 0) {
                try {
                    if (discard_partitions_) {
                        run_metadata_discard_partition_process(output_path_, index);
                    } else {
                        run_metadata_writer_partition_process(writer_config_, output_path_, partitions_, index);
                    }
                    _Exit(0);
                } catch (const std::exception& error) {
                    std::cerr << "metadata writer partition " << index << " failed: "
                              << error.what() << '\n';
                    _Exit(101);
                } catch (...) {
                    std::cerr << "metadata writer partition " << index << " failed\n";
                    _Exit(101);
                }
            }
            children_.push_back(child);
        }

        for (std::size_t index = 0; index < partitions_; ++index) {
            if (routed_source_pool_ != nullptr) {
                route_states_.push_back(std::make_unique<RouteSendState>(
                    *routed_source_pool_,
                    kMetadataPartitionTransportPoolSlots,
                    metadata_partition_socket_path(output_path_, index)));
                route_states_.back()->sender.start();
            } else {
                states_.push_back(std::make_unique<PartitionSendState>(
                    kMetadataPartitionTransportPoolSlots,
                    metadata_partition_socket_path(output_path_, index)));
                states_.back()->sender.start();
            }
        }
    }

    ~PartitionedMetadataWriter() {
        if (!closed_) {
            try {
                close();
            } catch (...) {
            }
        }
    }

    void write_batch(const std::vector<FileSpec>& files, const MetadataFolderRecord& folder) {
        if (routed_source_pool_ != nullptr) {
            throw std::runtime_error("partitioned metadata writer is in buffer routing mode");
        }
        if (!writer_config_.write_folders && (!writer_config_.write_files || files.empty())) {
            return;
        }

        const std::size_t partition = metadata_partition_for_path(folder.spec.rel_path, partitions_);
        std::lock_guard<std::mutex> lock(states_[partition]->mutex);
        if (writer_config_.write_folders) {
            append_generic_folder_record(partition, folder);
        }

        if (writer_config_.write_files && !files.empty()) {
            auto& batch = start_folder_batch(partition, folder);
            for (const auto& file : files) {
                if (!append_folder_metadata_batch_file(metadata_batch_buffer(states_[partition]->pool,
                                                                            states_[partition]->open_batch),
                                                       file)) {
                    flush_batch(partition);
                    auto& continued_batch = start_folder_batch(partition, folder);
                    if (!append_folder_metadata_batch_file(continued_batch, file)) {
                        throw std::runtime_error("metadata file record does not fit folder metadata batch buffer");
                    }
                }
            }
            (void)batch;
            flush_batch(partition);
        }
    }

    void route_flat_folder_buffer(const BufferHandle& handle, RawBufferPool& pool) {
        if (routed_source_pool_ == nullptr || route_states_.empty()) {
            throw std::runtime_error("partitioned metadata writer is not in buffer routing mode");
        }
        if (&pool != routed_source_pool_) {
            throw std::runtime_error("partitioned metadata router received buffer from unexpected pool");
        }
        if (handle.pool_id != kMetadataBatchBufferPoolId) {
            throw std::runtime_error("partitioned metadata router received non-metadata buffer");
        }
        const MetadataBatchBuffer& buffer = metadata_batch_buffer(pool, handle);
        if (!is_flat_folder_buffer(buffer)) {
            throw std::runtime_error("partitioned metadata router received non-flat-folder buffer");
        }

        const FlatFolderBufferInfo info = flat_folder_buffer_info(buffer);
        const std::size_t partition = metadata_partition_for_flat_folder_buffer(info, partitions_);
        RouteSendState& state = *route_states_[partition];
        std::lock_guard<std::mutex> lock(state.mutex);
        if (!state.queue.push_wait(handle)) {
            throw std::runtime_error("metadata route sender queue closed while pushing scan buffer");
        }
    }

    void close() {
        if (closed_) {
            return;
        }
        closed_ = true;
        if (routed_source_pool_ == nullptr) {
            for (std::size_t index = 0; index < partitions_; ++index) {
                std::lock_guard<std::mutex> lock(states_[index]->mutex);
                flush_batch(index);
                flush_generic_batch(index);
            }
        }
        for (auto& state : states_) {
            state->queue.close();
        }
        for (auto& state : route_states_) {
            state->queue.close();
        }
        std::exception_ptr sender_error;
        for (auto& state : states_) {
            try {
                state->sender.wait();
            } catch (...) {
                if (!sender_error) {
                    sender_error = std::current_exception();
                }
            }
        }
        for (auto& state : route_states_) {
            try {
                state->sender.wait();
            } catch (...) {
                if (!sender_error) {
                    sender_error = std::current_exception();
                }
            }
        }

        bool child_failed = false;
        for (const pid_t child : children_) {
            int status = 0;
            if (::waitpid(child, &status, 0) < 0) {
                throw std::runtime_error("failed waiting for metadata writer partition process");
            }
            if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
                child_failed = true;
            }
        }
        if (child_failed) {
            throw std::runtime_error("one or more metadata writer partition processes failed");
        }
        if (sender_error) {
            std::rethrow_exception(sender_error);
        }
        for (std::size_t index = 0; index < partitions_; ++index) {
            const auto [files, folders] = read_metadata_partition_report(
                metadata_partition_report_path(output_path_, index));
            files_written_ += files;
            folders_written_ += folders;
            std::filesystem::remove(metadata_partition_report_path(output_path_, index));
            std::filesystem::remove(metadata_partition_socket_path(output_path_, index));
        }
    }

    [[nodiscard]] std::uint64_t files_written() const {
        return files_written_;
    }

    [[nodiscard]] std::uint64_t folders_written() const {
        return folders_written_;
    }

    [[nodiscard]] std::vector<BufQueue*> route_queues() const {
        std::vector<BufQueue*> queues;
        queues.reserve(route_states_.size());
        for (const auto& state : route_states_) {
            queues.push_back(&state->queue);
        }
        return queues;
    }

    [[nodiscard]] std::size_t route_queue_capacity() const {
        std::size_t capacity = 0;
        for (const auto& state : route_states_) {
            capacity += state->queue.capacity();
        }
        return capacity;
    }

    [[nodiscard]] std::size_t route_queue_high_watermark() const {
        std::size_t high_watermark = 0;
        for (const auto& state : route_states_) {
            high_watermark += state->queue.high_watermark();
        }
        return high_watermark;
    }

    [[nodiscard]] bool route_queue_full() const {
        for (const auto& state : route_states_) {
            if (state->queue.high_watermark() >= state->queue.capacity()) {
                return true;
            }
        }
        return false;
    }

private:
    struct PartitionSendState {
        RawBufferPool pool;
        BufferPoolRegistry registry;
        BufQueue queue;
        BufferSenderJob sender;
        BufferHandle open_batch;
        bool has_open_batch = false;
        BufferHandle open_generic_batch;
        bool has_open_generic_batch = false;
        std::mutex mutex;

        PartitionSendState(std::size_t pool_slots, const std::filesystem::path& socket_path)
            : pool(kMetadataBatchBufferPoolId,
                   pool_slots,
                   sizeof(MetadataBatchBuffer),
                   alignof(MetadataBatchBuffer)),
              queue(pool_slots),
              sender(1U,
                     queue,
                     registry,
                     BufferTransportEndpoint::unix_socket(socket_path),
                     metadata_transport_payload_bytes) {
            registry.register_pool(pool);
        }
    };

    struct RouteSendState {
        BufferPoolRegistry registry;
        BufQueue queue;
        BufferSenderJob sender;
        std::mutex mutex;

        RouteSendState(RawBufferPool& source_pool, std::size_t queue_slots, const std::filesystem::path& socket_path)
            : queue(queue_slots),
              sender(1U,
                     queue,
                     registry,
                     BufferTransportEndpoint::unix_socket(socket_path),
                     metadata_transport_payload_bytes) {
            registry.register_pool(source_pool);
        }
    };

    MetadataBatchBuffer& acquire_batch(std::size_t partition) {
        PartitionSendState& state = *states_[partition];
        if (!state.has_open_batch) {
            state.open_batch = state.pool.acquire_wait();
            reset_metadata_batch(metadata_batch_buffer(state.pool, state.open_batch));
            state.has_open_batch = true;
        }
        return metadata_batch_buffer(state.pool, state.open_batch);
    }

    MetadataBatchBuffer& acquire_generic_batch(std::size_t partition) {
        PartitionSendState& state = *states_[partition];
        if (!state.has_open_generic_batch) {
            state.open_generic_batch = state.pool.acquire_wait();
            reset_metadata_batch(metadata_batch_buffer(state.pool, state.open_generic_batch));
            state.has_open_generic_batch = true;
        }
        return metadata_batch_buffer(state.pool, state.open_generic_batch);
    }

    MetadataBatchBuffer& start_folder_batch(std::size_t partition,
                                            const MetadataFolderRecord& folder) {
        PartitionSendState& state = *states_[partition];
        if (state.has_open_batch) {
            throw std::runtime_error("cannot start folder metadata batch while another batch is open");
        }
        state.open_batch = state.pool.acquire_wait();
        if (!reset_folder_metadata_batch(metadata_batch_buffer(state.pool, state.open_batch),
                                         folder,
                                         false)) {
            state.pool.release(state.open_batch);
            state.has_open_batch = false;
            throw std::runtime_error("metadata folder path does not fit metadata batch buffer");
        }
        state.has_open_batch = true;
        return metadata_batch_buffer(state.pool, state.open_batch);
    }

    void append_generic_folder_record(std::size_t partition, const MetadataFolderRecord& folder) {
        if (append_metadata_batch_folder(acquire_generic_batch(partition), folder)) {
            return;
        }
        flush_generic_batch(partition);
        if (!append_metadata_batch_folder(acquire_generic_batch(partition), folder)) {
            throw std::runtime_error("metadata folder record does not fit metadata batch buffer");
        }
    }

    void flush_batch(std::size_t partition) {
        PartitionSendState& state = *states_[partition];
        if (!state.has_open_batch) {
            return;
        }
        BufferHandle handle = state.open_batch;
        if (metadata_batch_buffer(state.pool, handle).record_count == 0U) {
            state.pool.release(handle);
        } else if (!state.queue.push_wait(handle)) {
            state.pool.release(handle);
            throw std::runtime_error("metadata sender queue closed while pushing scan batch");
        }
        state.has_open_batch = false;
    }

    void flush_generic_batch(std::size_t partition) {
        PartitionSendState& state = *states_[partition];
        if (!state.has_open_generic_batch) {
            return;
        }
        BufferHandle handle = state.open_generic_batch;
        if (metadata_batch_buffer(state.pool, handle).record_count == 0U) {
            state.pool.release(handle);
        } else if (!state.queue.push_wait(handle)) {
            state.pool.release(handle);
            throw std::runtime_error("metadata sender queue closed while pushing generic metadata batch");
        }
        state.has_open_generic_batch = false;
    }

    MetadataRecordWriterConfig writer_config_;
    std::filesystem::path output_path_;
    std::size_t partitions_;
    RawBufferPool* routed_source_pool_ = nullptr;
    bool discard_partitions_ = false;
    std::vector<std::unique_ptr<PartitionSendState>> states_;
    std::vector<std::unique_ptr<RouteSendState>> route_states_;
    std::vector<pid_t> children_;
    std::uint64_t files_written_ = 0;
    std::uint64_t folders_written_ = 0;
    bool closed_ = false;
};

struct DataReadBenchmarkSnapshot {
    std::size_t files_found = 0;
    std::size_t folders_found = 0;
    std::size_t files_read = 0;
    std::size_t files_failed = 0;
    std::uint64_t logical_size_bytes = 0;
    std::uint64_t bytes_read = 0;
    std::size_t data_buffer_slots = 0;
    std::size_t data_queue_depth = 0;
    double elapsed_seconds = 0.0;
    double bytes_per_second = 0.0;
    double gigabits_per_second = 0.0;
    double files_per_second = 0.0;
    double small_files_per_second = 0.0;
    double large_files_per_second = 0.0;
    double small_gigabits_per_second = 0.0;
    double large_gigabits_per_second = 0.0;
    std::size_t folders_written = 0;
    double folders_per_second = 0.0;
    std::size_t small_files_found = 0;
    std::size_t large_files_found = 0;
    std::size_t small_files_read = 0;
    std::size_t large_files_read = 0;
    std::uint64_t small_bytes_read = 0;
    std::uint64_t large_bytes_read = 0;
    std::size_t recon_files_found = 0;
    std::size_t recon_folders_found = 0;
    std::size_t recon_small_files_found = 0;
    std::size_t recon_large_files_found = 0;
    std::uint64_t recon_logical_size_bytes = 0;
    std::uint64_t small_logical_size_bytes = 0;
    std::uint64_t large_logical_size_bytes = 0;
    std::uint64_t recon_small_logical_size_bytes = 0;
    std::uint64_t recon_large_logical_size_bytes = 0;
    bool recon_completed = false;
};

struct DataReadBenchmarkStats {
    std::atomic<std::size_t> files_found {0};
    std::atomic<std::size_t> folders_found {0};
    std::atomic<std::size_t> files_read {0};
    std::atomic<std::size_t> files_failed {0};
    std::atomic<std::uint64_t> logical_size_bytes {0};
    std::atomic<std::uint64_t> bytes_read {0};
    std::atomic<std::size_t> small_files_found {0};
    std::atomic<std::size_t> large_files_found {0};
    std::atomic<std::size_t> small_files_read {0};
    std::atomic<std::size_t> large_files_read {0};
    std::atomic<std::uint64_t> small_bytes_read {0};
    std::atomic<std::uint64_t> large_bytes_read {0};
    std::atomic<std::uint64_t> small_logical_size_bytes {0};
    std::atomic<std::uint64_t> large_logical_size_bytes {0};
    std::uint32_t print_interval_seconds = 5;
    std::chrono::steady_clock::time_point started_at {};
    std::chrono::steady_clock::time_point last_print_at {};
    std::mutex print_mutex;
    std::mutex printer_mutex;
    std::condition_variable printer_cv;
    std::atomic<bool> printer_done {false};
};

struct HashInventorySnapshot {
    std::size_t files_found = 0;
    std::size_t folders_found = 0;
    std::size_t files_hashed = 0;
    std::size_t files_failed = 0;
    std::uint64_t logical_size_bytes = 0;
    std::uint64_t bytes_read = 0;
    std::uint64_t bytes_hashed = 0;
    double elapsed_seconds = 0.0;
    double read_elapsed_seconds = 0.0;
    double read_bytes_per_second = 0.0;
    double read_gigabits_per_second = 0.0;
    double bytes_per_second = 0.0;
};

enum class HashMode {
    file,
    blocks,
};

struct HashInventoryStats {
    std::atomic<std::size_t> files_found {0};
    std::atomic<std::size_t> folders_found {0};
    std::atomic<std::size_t> files_hashed {0};
    std::atomic<std::size_t> files_failed {0};
    std::atomic<std::uint64_t> logical_size_bytes {0};
    std::atomic<std::uint64_t> bytes_read {0};
    std::atomic<std::uint64_t> bytes_hashed {0};
    std::atomic<std::uint64_t> read_elapsed_nanoseconds {0};
    std::chrono::steady_clock::time_point started_at {};
};

enum class HashChunkKind {
    begin,
    data,
    end,
    failed,
};

struct HashChunkMessage {
    HashChunkKind kind = HashChunkKind::data;
    std::uint64_t file_id = 0;
    std::uint64_t block_index = 0;
    std::uint64_t offset = 0;
    std::uint64_t data_len = 0;
    FileSpec file;
    DataSlotHandle handle;
    bool has_data_slot = false;
};

struct HashChunkShardQueue {
    std::mutex mutex;
    std::condition_variable cv_not_empty;
    std::condition_variable cv_not_full;
    std::deque<HashChunkMessage> messages;
    std::size_t max_entries = 1;
    std::size_t queued_bytes = 0;
    bool input_done = false;
    bool stop = false;
    std::exception_ptr error;
};

struct ActiveHashFile {
    FileSpec file;
    Md5State md5;
    Sha256State sha256;
    Hash64State xxh64;
    Xxh3_64State xxh3_64;
    Xxh3_128State xxh3_128;
    std::uint64_t bytes_hashed = 0;
    std::uint64_t next_data_offset = 0;
    std::map<std::uint64_t, DataSlotHandle> pending_data_slots;
    std::vector<std::string> block_hashes;
    std::uint64_t next_block_index = 0;
    std::uint64_t current_block_bytes = 0;
    std::uint64_t current_block_expected = 0;
};

struct HashSpeedWorkerResult {
    std::uint64_t bytes_hashed = 0;
    std::uint64_t iterations = 0;
    std::uint64_t digest_mix = 0;
};

void append_u32(std::string& out, std::uint32_t value) {
    out.push_back(static_cast<char>((value >> 24U) & 0xFFU));
    out.push_back(static_cast<char>((value >> 16U) & 0xFFU));
    out.push_back(static_cast<char>((value >> 8U) & 0xFFU));
    out.push_back(static_cast<char>(value & 0xFFU));
}

void append_u64(std::string& out, std::uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        out.push_back(static_cast<char>((value >> static_cast<unsigned>(shift)) & 0xFFU));
    }
}

void append_bool(std::string& out, bool value) {
    out.push_back(value ? '\x01' : '\x00');
}

void write_u32_be(char* out, std::size_t& offset, std::uint32_t value) {
    out[offset++] = static_cast<char>((value >> 24U) & 0xFFU);
    out[offset++] = static_cast<char>((value >> 16U) & 0xFFU);
    out[offset++] = static_cast<char>((value >> 8U) & 0xFFU);
    out[offset++] = static_cast<char>(value & 0xFFU);
}

void write_u64_be(char* out, std::size_t& offset, std::uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        out[offset++] = static_cast<char>((value >> static_cast<unsigned>(shift)) & 0xFFU);
    }
}

void write_u32_be_at(char* out, std::size_t offset, std::uint32_t value) {
    out[offset++] = static_cast<char>((value >> 24U) & 0xFFU);
    out[offset++] = static_cast<char>((value >> 16U) & 0xFFU);
    out[offset++] = static_cast<char>((value >> 8U) & 0xFFU);
    out[offset] = static_cast<char>(value & 0xFFU);
}

void append_string(std::string& out, std::string_view value) {
    append_u32(out, static_cast<std::uint32_t>(value.size()));
    out.append(value);
}

std::uint32_t read_u32(std::string_view input, std::size_t& offset) {
    if (offset + 4U > input.size()) {
        throw std::runtime_error("protocol underflow reading u32");
    }
    const auto* bytes = reinterpret_cast<const unsigned char*>(input.data() + offset);
    offset += 4U;
    return (static_cast<std::uint32_t>(bytes[0]) << 24U) |
           (static_cast<std::uint32_t>(bytes[1]) << 16U) |
           (static_cast<std::uint32_t>(bytes[2]) << 8U) |
           static_cast<std::uint32_t>(bytes[3]);
}

std::uint64_t read_u64(std::string_view input, std::size_t& offset) {
    if (offset + 8U > input.size()) {
        throw std::runtime_error("protocol underflow reading u64");
    }
    const auto* bytes = reinterpret_cast<const unsigned char*>(input.data() + offset);
    offset += 8U;
    std::uint64_t value = 0;
    for (int i = 0; i < 8; ++i) {
        value = (value << 8U) | static_cast<std::uint64_t>(bytes[i]);
    }
    return value;
}

bool read_bool(std::string_view input, std::size_t& offset) {
    if (offset + 1U > input.size()) {
        throw std::runtime_error("protocol underflow reading bool");
    }
    return input[offset++] != '\0';
}

template <std::size_t Capacity>
void read_fixed_string(std::string_view input, std::size_t& offset, FixedString<Capacity>& value) {
    const std::uint32_t length = read_u32(input, offset);
    if (length > Capacity) {
        throw std::runtime_error("protocol string exceeds fixed buffer capacity");
    }
    if (offset + length > input.size()) {
        throw std::runtime_error("protocol underflow reading fixed string");
    }
    value.assign(input.substr(offset, length));
    offset += length;
}

void send_priority_payload(int fd, PriorityMessageType type, const std::string& payload) {
    std::string header;
    header.reserve(8);
    append_u32(header, static_cast<std::uint32_t>(type));
    append_u32(header, static_cast<std::uint32_t>(payload.size()));
    write_all(fd, header.data(), header.size());
    if (!payload.empty()) {
        write_all(fd, payload.data(), payload.size());
    }
}

bool read_priority_payload(int fd, PriorityMessageType& type, std::string& payload) {
    std::array<char, 8> header {};
    if (!read_exact_or_eof(fd, header.data(), header.size())) {
        return false;
    }
    std::size_t offset = 0;
    const std::string_view header_view(header.data(), header.size());
    type = static_cast<PriorityMessageType>(read_u32(header_view, offset));
    const std::uint32_t payload_size = read_u32(header_view, offset);
    payload.assign(payload_size, '\0');
    if (payload_size != 0U && !read_exact_or_eof(fd, payload.data(), payload.size())) {
        throw std::runtime_error("unexpected EOF while reading priority payload");
    }
    return true;
}

void send_session_start(int fd, const EngineConfig& config) {
    std::string payload;
    append_u32(payload, 1U);
    append_u32(payload, static_cast<std::uint32_t>(config.large_chunk_bytes));
    append_bool(payload, config.skip_verify);
    send_priority_payload(fd, PriorityMessageType::session_start, payload);
}

void send_file_record(int fd, const FileRecordMessage& record) {
    std::string payload;
    append_u64(payload, record.file_id);
    append_u64(payload, record.folder_hash);
    append_u64(payload, record.size);
    append_u64(payload, record.mtime);
    append_u32(payload, record.mode);
    append_u32(payload, record.uid);
    append_u32(payload, record.gid);
    append_string(payload, record.rel_path);
    send_priority_payload(fd, PriorityMessageType::file_record, payload);
}

FileRecordMessage decode_file_record(std::string_view payload) {
    std::size_t offset = 0;
    FileRecordMessage record;
    record.file_id = read_u64(payload, offset);
    record.folder_hash = read_u64(payload, offset);
    record.size = read_u64(payload, offset);
    record.mtime = read_u64(payload, offset);
    record.mode = read_u32(payload, offset);
    record.uid = read_u32(payload, offset);
    record.gid = read_u32(payload, offset);
    read_fixed_string(payload, offset, record.rel_path);
    return record;
}

void send_file_decision(int fd, const FileDecisionMessage& decision) {
    std::string payload;
    append_u64(payload, decision.file_id);
    append_bool(payload, decision.skip);
    send_priority_payload(fd, PriorityMessageType::file_decision, payload);
}

FileDecisionMessage decode_file_decision(std::string_view payload) {
    std::size_t offset = 0;
    FileDecisionMessage decision;
    decision.file_id = read_u64(payload, offset);
    decision.skip = read_bool(payload, offset);
    return decision;
}

void send_directory_record(int fd, const DirectoryRecordMessage& record) {
    std::string payload;
    append_u64(payload, record.mtime);
    append_u32(payload, record.mode);
    append_u32(payload, record.uid);
    append_u32(payload, record.gid);
    append_string(payload, record.rel_path);
    send_priority_payload(fd, PriorityMessageType::directory_record, payload);
}

DirectoryRecordMessage decode_directory_record(std::string_view payload) {
    std::size_t offset = 0;
    DirectoryRecordMessage record;
    record.mtime = read_u64(payload, offset);
    record.mode = read_u32(payload, offset);
    record.uid = read_u32(payload, offset);
    record.gid = read_u32(payload, offset);
    read_fixed_string(payload, offset, record.rel_path);
    return record;
}

void send_file_ack(int fd, const FileAckMessage& ack) {
    std::string payload;
    append_u64(payload, ack.file_id);
    append_bool(payload, ack.hash_verified);
    append_u64(payload, ack.bytes_written);
    send_priority_payload(fd, PriorityMessageType::file_ack, payload);
}

FileAckMessage decode_file_ack(std::string_view payload) {
    std::size_t offset = 0;
    FileAckMessage ack;
    ack.file_id = read_u64(payload, offset);
    ack.hash_verified = read_bool(payload, offset);
    ack.bytes_written = read_u64(payload, offset);
    return ack;
}

void send_session_end(int fd) {
    send_priority_payload(fd, PriorityMessageType::session_end, {});
}

void send_pause(int fd) {
    send_priority_payload(fd, PriorityMessageType::pause, {});
}

void send_resume(int fd) {
    send_priority_payload(fd, PriorityMessageType::resume, {});
}

void send_data_slot(int fd, const DataSlotPool& pool, const DataSlotHandle& handle) {
    const DataBufTrailer& trailer = pool.trailer(handle);

    std::string header;
    header.reserve(80);
    append_u64(header, trailer.file_id);
    append_u64(header, trailer.folder_hash);
    append_u64(header, trailer.data_offset);
    append_u64(header, trailer.data_len);
    append_u64(header, trailer.file_size);
    append_u64(header, trailer.data_hash);
    append_u64(header, trailer.mtime);
    append_u32(header, trailer.mode);
    append_u32(header, trailer.uid);
    append_u32(header, trailer.gid);
    append_u32(header, trailer.chunk_hash);
    append_u32(header, trailer.flags);
    append_u32(header, static_cast<std::uint32_t>(trailer.rel_path.size()));
    write_all(fd, header.data(), header.size());
    if (!trailer.rel_path.empty()) {
        write_all(fd, trailer.rel_path.data(), trailer.rel_path.size());
    }
    if (trailer.data_len != 0U) {
        write_all(fd, pool.data(handle), static_cast<std::size_t>(trailer.data_len));
    }
}

bool read_data_slot(int fd, DataSlotPool& pool, DataSlotHandle& handle) {
    std::array<char, 80> header {};
    if (!read_exact_or_eof(fd, header.data(), header.size())) {
        return false;
    }

    std::size_t offset = 0;
    const std::string_view header_view(header.data(), header.size());
    const std::uint64_t file_id = read_u64(header_view, offset);
    const std::uint64_t folder_hash = read_u64(header_view, offset);
    const std::uint64_t data_offset = read_u64(header_view, offset);
    const std::uint64_t data_len = read_u64(header_view, offset);
    const std::uint64_t file_size = read_u64(header_view, offset);
    const std::uint64_t data_hash = read_u64(header_view, offset);
    const std::uint64_t mtime = read_u64(header_view, offset);
    const std::uint32_t mode = read_u32(header_view, offset);
    const std::uint32_t uid = read_u32(header_view, offset);
    const std::uint32_t gid = read_u32(header_view, offset);
    const std::uint32_t chunk_hash = read_u32(header_view, offset);
    const std::uint32_t flags = read_u32(header_view, offset);
    const std::uint32_t path_length = read_u32(header_view, offset);

    const DataSlotClass slot_class = (flags & kFlagSmallFile) != 0U ? DataSlotClass::small : DataSlotClass::large;
    handle = pool.acquire_wait_or_throw(slot_class, static_cast<std::size_t>(data_len));
    DataBufTrailer& trailer = pool.trailer(handle);
    trailer.file_id = file_id;
    trailer.folder_hash = folder_hash;
    trailer.data_offset = data_offset;
    trailer.data_len = data_len;
    trailer.file_size = file_size;
    trailer.data_hash = data_hash;
    trailer.mtime = mtime;
    trailer.mode = mode;
    trailer.uid = uid;
    trailer.gid = gid;
    trailer.chunk_hash = chunk_hash;
    trailer.flags = flags;
    trailer.rel_path.resize_for_overwrite(path_length);
    trailer.slot_valid = kSlotValid;

    if (path_length != 0U && !read_exact_or_eof(fd, trailer.rel_path.data(), path_length)) {
        pool.release(handle);
        throw std::runtime_error("unexpected EOF while reading chunk path");
    }
    if (data_len != 0U &&
        !read_exact_or_eof(fd, pool.data(handle), static_cast<std::size_t>(data_len))) {
        pool.release(handle);
        throw std::runtime_error("unexpected EOF while reading chunk payload");
    }
    return true;
}

struct PackedSmallFileEntry {
    FileRecordMessage record;
    std::uint64_t data_hash = 0;
    std::string_view data;
};

constexpr std::size_t kPackedSmallFileCountBytes = 4;
constexpr std::size_t kPackedSmallFileEntryFixedBytes =
    8 + 8 + 8 + 8 + 8 + 4 + 4 + 4 + 4;

bool is_packed_small_file_slot(const DataSlotPool& pool, const DataSlotHandle& handle) {
    return (pool.trailer(handle).flags & kFlagPackedSmallFiles) != 0U;
}

std::size_t packed_small_file_entry_bytes(const PreparedTransfer& prepared) {
    return kPackedSmallFileEntryFixedBytes +
           prepared.record.rel_path.size() +
           static_cast<std::size_t>(prepared.record.size);
}

bool can_pack_small_file(const PreparedTransfer& prepared, const EngineConfig& config) {
    return !prepared.cached &&
           prepared.record.size <= config.small_file_threshold &&
           packed_small_file_entry_bytes(prepared) + kPackedSmallFileCountBytes <= config.large_chunk_bytes;
}

std::vector<std::uint64_t> packed_small_file_ids(const DataSlotPool& pool, const DataSlotHandle& handle) {
    const DataBufTrailer& trailer = pool.trailer(handle);
    std::string_view payload(pool.data(handle), static_cast<std::size_t>(trailer.data_len));
    std::size_t offset = 0;
    const std::uint32_t count = read_u32(payload, offset);
    std::vector<std::uint64_t> ids;
    ids.reserve(count);
    for (std::uint32_t index = 0; index < count; ++index) {
        const std::uint64_t file_id = read_u64(payload, offset);
        const std::uint64_t data_len = read_u64(payload, offset);
        (void)read_u64(payload, offset);
        (void)read_u64(payload, offset);
        (void)read_u64(payload, offset);
        (void)read_u32(payload, offset);
        (void)read_u32(payload, offset);
        (void)read_u32(payload, offset);
        const std::uint32_t path_len = read_u32(payload, offset);
        if (offset + path_len + data_len > payload.size()) {
            throw std::runtime_error("packed small file payload is truncated");
        }
        offset += path_len + static_cast<std::size_t>(data_len);
        ids.push_back(file_id);
    }
    if (offset != payload.size()) {
        throw std::runtime_error("packed small file payload has trailing bytes");
    }
    return ids;
}

std::vector<PackedSmallFileEntry> decode_packed_small_file_entries(const DataSlotPool& pool,
                                                                   const DataSlotHandle& handle) {
    const DataBufTrailer& trailer = pool.trailer(handle);
    std::string_view payload(pool.data(handle), static_cast<std::size_t>(trailer.data_len));
    std::size_t offset = 0;
    const std::uint32_t count = read_u32(payload, offset);
    std::vector<PackedSmallFileEntry> entries;
    entries.reserve(count);
    for (std::uint32_t index = 0; index < count; ++index) {
        PackedSmallFileEntry entry;
        entry.record.file_id = read_u64(payload, offset);
        const std::uint64_t data_len = read_u64(payload, offset);
        entry.record.size = read_u64(payload, offset);
        entry.data_hash = read_u64(payload, offset);
        entry.record.mtime = read_u64(payload, offset);
        entry.record.mode = read_u32(payload, offset);
        entry.record.uid = read_u32(payload, offset);
        entry.record.gid = read_u32(payload, offset);
        const std::uint32_t path_len = read_u32(payload, offset);
        if (path_len > entry.record.rel_path.capacity()) {
            throw std::runtime_error("packed small file path exceeds protocol capacity");
        }
        if (offset + path_len + data_len > payload.size()) {
            throw std::runtime_error("packed small file payload is truncated");
        }
        entry.record.rel_path.assign(payload.substr(offset, path_len));
        offset += path_len;
        entry.data = payload.substr(offset, static_cast<std::size_t>(data_len));
        offset += static_cast<std::size_t>(data_len);
        entries.push_back(entry);
    }
    if (offset != payload.size()) {
        throw std::runtime_error("packed small file payload has trailing bytes");
    }
    return entries;
}

std::size_t path_depth(std::string_view rel_path) {
    if (rel_path.empty()) {
        return 0;
    }
    return 1U + static_cast<std::size_t>(std::count(rel_path.begin(), rel_path.end(), '/'));
}

bool target_matches_record(const NfsBackend& backend, const FileRecordMessage& record) {
    if (!backend.metadata_matches(record.rel_path, record.size, record.mtime)) {
        return false;
    }

    const auto existing = backend.stat_path(record.rel_path);
    if (!existing.has_value()) {
        return false;
    }
    return existing->mode == record.mode && existing->uid == record.uid && existing->gid == record.gid;
}

FileRecordMessage make_file_record_message(const FileSpec& file) {
    const RecBuf record = make_recbuf(file);
    FileRecordMessage message;
    message.file_id = record.own_hash;
    message.folder_hash = record.folder_hash;
    message.rel_path = record.rel_path;
    message.size = record.size;
    message.mtime = record.mtime;
    message.mode = record.mode;
    message.uid = record.uid;
    message.gid = record.gid;
    return message;
}

DirectoryRecordMessage make_directory_record_message(const FileSpec& directory) {
    DirectoryRecordMessage message;
    message.rel_path = normalize_path(directory.rel_path);
    message.mtime = directory.mtime;
    message.mode = directory.mode;
    message.uid = directory.uid;
    message.gid = directory.gid;
    return message;
}

FileSpec make_directory_spec(const DirectoryRecordMessage& record) {
    FileSpec spec;
    spec.rel_path = normalize_path(record.rel_path.view());
    spec.mtime = record.mtime;
    spec.mode = record.mode;
    spec.uid = record.uid;
    spec.gid = record.gid;
    return spec;
}

struct SessionStartInfo {
    std::uint64_t large_chunk_bytes = kLargeChunkBytes;
    bool skip_verify = false;
};

SessionStartInfo decode_session_start(std::string_view payload) {
    std::size_t offset = 0;
    (void)read_u32(payload, offset);
    SessionStartInfo info;
    info.large_chunk_bytes = read_u32(payload, offset);
    info.skip_verify = read_bool(payload, offset);
    return info;
}

double percentage_of(std::uint64_t value, std::uint64_t total) {
    if (total == 0) {
        return 0.0;
    }
    return static_cast<double>(value) * 100.0 / static_cast<double>(total);
}

std::uint64_t receiver_queue_capacity_bytes(const ReceiverSharedState& state) {
    if (state.runtime.backpressure_window_bytes != 0) {
        return state.runtime.backpressure_window_bytes;
    }
    return std::max<std::uint64_t>(state.session_large_chunk_bytes * 8U, kLargeChunkBytes * 8U);
}

std::string json_quote(std::string_view value) {
    std::string out;
    out.reserve(value.size() + 2U);
    out.push_back('"');
    for (const char ch : value) {
        switch (ch) {
            case '\\':
                out += "\\\\";
                break;
            case '"':
                out += "\\\"";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                out.push_back(ch);
                break;
        }
    }
    out.push_back('"');
    return out;
}

std::string utc_time_string(std::chrono::system_clock::time_point time_point) {
    const std::time_t seconds = std::chrono::system_clock::to_time_t(time_point);
    std::tm tm {};
#if defined(_WIN32)
    gmtime_s(&tm, &seconds);
#else
    gmtime_r(&seconds, &tm);
#endif
    std::ostringstream out;
    out << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    return out.str();
}

std::string compact_clock_time_string(std::chrono::system_clock::time_point time_point) {
    const std::time_t seconds = std::chrono::system_clock::to_time_t(time_point);
    std::tm tm {};
#if defined(_WIN32)
    localtime_s(&tm, &seconds);
#else
    localtime_r(&seconds, &tm);
#endif
    std::ostringstream out;
    out << std::put_time(&tm, "%I:%M%p");
    std::string value = out.str();
    if (!value.empty() && value.front() == '0') {
        value.erase(value.begin());
    }
    for (char& ch : value) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    if (value.ends_with("am") || value.ends_with("pm")) {
        value.pop_back();
    }
    return value;
}

std::string compact_eta_duration(double seconds) {
    if (!std::isfinite(seconds) || seconds < 0.0) {
        return "unknown";
    }
    std::ostringstream out;
    if (seconds >= 3600.0) {
        out << std::fixed << std::setprecision(1) << (seconds / 3600.0) << "h";
    } else if (seconds >= 60.0) {
        out << std::fixed << std::setprecision(1) << (seconds / 60.0) << "m";
    } else {
        out << std::fixed << std::setprecision(0) << seconds << "s";
    }
    return out.str();
}

std::string human_count(double value, std::string_view suffix = "") {
    const double abs_value = std::abs(value);
    const char* unit = "";
    double scaled = value;
    if (abs_value >= 1'000'000'000.0) {
        scaled = value / 1'000'000'000.0;
        unit = "B";
    } else if (abs_value >= 1'000'000.0) {
        scaled = value / 1'000'000.0;
        unit = "M";
    } else if (abs_value >= 1'000.0) {
        scaled = value / 1'000.0;
        unit = "K";
    }
    std::ostringstream out;
    out << std::fixed << std::setprecision(abs_value >= 1'000.0 ? 1 : 0)
        << scaled << unit << suffix;
    return out.str();
}

std::string human_count_rate(double value) {
    return human_count(value, "/s");
}

std::string human_capacity(std::uint64_t bytes) {
    const char* units[] = {"B", "K", "M", "G", "T", "P"};
    double value = static_cast<double>(bytes);
    std::size_t unit_index = 0;
    while (value >= 1000.0 && unit_index + 1U < std::size(units)) {
        value /= 1000.0;
        ++unit_index;
    }
    std::ostringstream out;
    out << std::fixed << std::setprecision(unit_index == 0U ? 0 : 1)
        << value << units[unit_index];
    return out.str();
}

std::string human_gbit_rate(double bytes_per_second) {
    const double gbit = bytes_per_second * 8.0 / 1'000'000'000.0;
    std::ostringstream out;
    out << std::fixed << std::setprecision(gbit >= 100.0 ? 0 : 1)
        << gbit << "Gbit/s";
    return out.str();
}

std::string percent_string(double percent) {
    std::ostringstream out;
    out << std::fixed << std::setprecision(percent >= 10.0 ? 0 : 1)
        << percent << "%";
    return out.str();
}

std::uint64_t unix_time_nanoseconds(std::chrono::system_clock::time_point time_point) {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(time_point.time_since_epoch()).count());
}

MetadataScanRunInfo make_metadata_scan_run_info(const std::filesystem::path& source_root,
                                                bool recursive,
                                                const std::string& output_format,
                                                const std::string& metadata_records,
                                                std::size_t meta_reader_threads,
                                                std::size_t metadata_async_depth,
                                                std::size_t record_buffer_slots,
                                                double max_duration_seconds,
                                                std::uint32_t stats_interval_seconds) {
    const auto started = std::chrono::system_clock::now();
    const std::uint64_t started_ns = unix_time_nanoseconds(started);
    MetadataScanRunInfo info;
    info.run_id = std::to_string(started_ns);
    info.started_at_utc = utc_time_string(started);
    info.started_unix_ns = started_ns;
    info.source_root = source_root.string();

    std::ostringstream settings;
    settings << '{'
             << "\"app\":\"hypersync\","
             << "\"version\":" << json_quote(kVersion) << ','
             << "\"mode\":\"metadata_scan\","
             << "\"source\":" << json_quote(source_root.string()) << ','
             << "\"recursive\":" << (recursive ? "true" : "false") << ','
             << "\"output_format\":" << json_quote(output_format) << ','
             << "\"records\":" << json_quote(metadata_records) << ','
             << "\"meta_reader_threads\":" << meta_reader_threads << ','
             << "\"metadata_async_depth\":" << metadata_async_depth << ','
             << "\"record_buffer_slots\":" << record_buffer_slots << ','
             << "\"max_duration_seconds\":" << max_duration_seconds << ','
             << "\"stats_interval_seconds\":" << stats_interval_seconds << '}';
    info.settings_json = settings.str();
    return info;
}

void enqueue_flat_folder_work(FlatMetadataWorkQueue& queue, std::vector<FileSpec>&& folders) {
    if (folders.empty()) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        if (queue.done || (queue.stop_at.has_value() && std::chrono::steady_clock::now() >= *queue.stop_at)) {
            queue.done = true;
            return;
        }
        for (auto& folder : folders) {
            queue.folders.push_back(std::move(folder));
        }
    }
    queue.cv.notify_all();
}

void finish_flat_folder_work(FlatMetadataWorkQueue& queue) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        if (queue.active != 0) {
            --queue.active;
        }
        if ((queue.stop_at.has_value() && std::chrono::steady_clock::now() >= *queue.stop_at) ||
            (queue.folders.empty() && queue.active == 0)) {
            queue.done = true;
            if (queue.stop_at.has_value()) {
                queue.folders.clear();
            }
        }
    }
    queue.cv.notify_all();
}

void fail_flat_folder_work(FlatMetadataWorkQueue& queue, std::exception_ptr error = std::current_exception()) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        if (queue.active != 0) {
            --queue.active;
        }
        queue.done = true;
        if (!queue.error && error != nullptr) {
            queue.error = error;
        }
    }
    queue.cv.notify_all();
}

void request_flat_folder_stop(FlatMetadataWorkQueue& queue) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.done = true;
        queue.folders.clear();
    }
    queue.cv.notify_all();
}

bool flat_metadata_scan_should_stop(const FlatMetadataWorkQueue& queue) {
    return queue.stop_at.has_value() && std::chrono::steady_clock::now() >= *queue.stop_at;
}

std::optional<FileSpec> take_flat_folder_work(FlatMetadataWorkQueue& queue, bool wait_for_work) {
    std::unique_lock<std::mutex> lock(queue.mutex);
    const auto ready = [&queue]() {
        return queue.done || queue.error || !queue.folders.empty() ||
               (queue.stop_at.has_value() && std::chrono::steady_clock::now() >= *queue.stop_at);
    };

    if (wait_for_work) {
        if (queue.stop_at.has_value()) {
            queue.cv.wait_until(lock, *queue.stop_at, ready);
        } else {
            queue.cv.wait(lock, ready);
        }
    } else if (!ready()) {
        return std::nullopt;
    }

    if (queue.stop_at.has_value() && std::chrono::steady_clock::now() >= *queue.stop_at) {
        queue.done = true;
        queue.folders.clear();
    }
    if (queue.error || queue.done || queue.folders.empty()) {
        return std::nullopt;
    }

    FileSpec folder = std::move(queue.folders.front());
    queue.folders.pop_front();
    ++queue.active;
    return folder;
}

bool enqueue_diff_target_folder(DiffTargetFolderQueue& queue, FileSpec folder) {
    std::unique_lock<std::mutex> lock(queue.mutex);
    queue.cv.wait(lock, [&queue]() {
        return queue.done || queue.error || queue.folders.size() < queue.max_entries;
    });
    if (queue.done || queue.error) {
        queue.cv.notify_all();
        return false;
    }
    queue.folders.push_back(std::move(folder));
    lock.unlock();
    queue.cv.notify_all();
    return true;
}

bool enqueue_diff_target_folders(DiffTargetFolderQueue& queue, std::vector<FileSpec>&& folders) {
    for (auto& folder : folders) {
        if (!enqueue_diff_target_folder(queue, std::move(folder))) {
            return false;
        }
    }
    return true;
}

std::optional<FileSpec> take_diff_target_folder_work(DiffTargetFolderQueue& queue, bool wait_for_work) {
    std::unique_lock<std::mutex> lock(queue.mutex);
    const auto ready = [&queue]() {
        return queue.done || queue.error || !queue.folders.empty() || queue.input_done;
    };
    if (wait_for_work) {
        queue.cv.wait(lock, ready);
    } else if (!ready()) {
        return std::nullopt;
    }
    if (queue.error || queue.done || queue.folders.empty()) {
        return std::nullopt;
    }

    FileSpec folder = std::move(queue.folders.front());
    queue.folders.pop_front();
    ++queue.active;
    lock.unlock();
    queue.cv.notify_all();
    return folder;
}

void finish_diff_target_folder_work(DiffTargetFolderQueue& queue) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        if (queue.active != 0) {
            --queue.active;
        }
        if (queue.input_done && queue.folders.empty() && queue.active == 0) {
            queue.done = true;
        }
    }
    queue.cv.notify_all();
}

void mark_diff_target_input_done(DiffTargetFolderQueue& queue) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.input_done = true;
        if (queue.folders.empty() && queue.active == 0) {
            queue.done = true;
        }
    }
    queue.cv.notify_all();
}

void fail_diff_target_folder_work(DiffTargetFolderQueue& queue,
                                  std::exception_ptr error = std::current_exception()) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        if (queue.active != 0) {
            --queue.active;
        }
        queue.done = true;
        queue.folders.clear();
        if (!queue.error && error != nullptr) {
            queue.error = error;
        }
    }
    queue.cv.notify_all();
}

bool diff_target_folder_work_idle(DiffTargetFolderQueue& queue) {
    std::lock_guard<std::mutex> lock(queue.mutex);
    return (queue.done || (queue.folders.empty() && queue.active == 0)) && queue.error == nullptr;
}

void configure_diff_batch_queue(DiffBatchQueue& queue,
                                std::size_t producers,
                                std::size_t max_entries) {
    std::lock_guard<std::mutex> lock(queue.mutex);
    queue.producers_remaining = producers;
    queue.max_entries = std::max<std::size_t>(1U, max_entries);
    queue.done = producers == 0U;
}

bool push_diff_batch(DiffBatchQueue& queue, FlatFolderScanBatch batch) {
    std::unique_lock<std::mutex> lock(queue.mutex);
    queue.cv_not_full.wait(lock, [&queue]() {
        return queue.done || queue.error || queue.batches.size() < queue.max_entries;
    });
    if (queue.done || queue.error) {
        queue.cv_not_empty.notify_all();
        queue.cv_not_full.notify_all();
        return false;
    }
    queue.batches.push_back(std::move(batch));
    lock.unlock();
    queue.cv_not_empty.notify_one();
    return true;
}

std::optional<FlatFolderScanBatch> take_diff_batch(DiffBatchQueue& queue, bool wait_for_work) {
    std::unique_lock<std::mutex> lock(queue.mutex);
    const auto ready = [&queue]() {
        return queue.done || queue.error || !queue.batches.empty();
    };
    if (wait_for_work) {
        queue.cv_not_empty.wait(lock, ready);
    } else if (!ready()) {
        return std::nullopt;
    }
    if (queue.batches.empty()) {
        return std::nullopt;
    }
    FlatFolderScanBatch batch = std::move(queue.batches.front());
    queue.batches.pop_front();
    lock.unlock();
    queue.cv_not_full.notify_one();
    return batch;
}

void finish_diff_batch_producer(DiffBatchQueue& queue) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        if (queue.producers_remaining != 0U) {
            --queue.producers_remaining;
        }
        if (queue.producers_remaining == 0U) {
            queue.done = true;
        }
    }
    queue.cv_not_empty.notify_all();
    queue.cv_not_full.notify_all();
}

void fail_diff_batch_queue(DiffBatchQueue& queue,
                           std::exception_ptr error = std::current_exception()) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.done = true;
        queue.batches.clear();
        if (!queue.error && error != nullptr) {
            queue.error = error;
        }
    }
    queue.cv_not_empty.notify_all();
    queue.cv_not_full.notify_all();
}

bool diff_batch_queue_drained(DiffBatchQueue& queue) {
    std::lock_guard<std::mutex> lock(queue.mutex);
    return queue.done && queue.batches.empty();
}

bool diff_batch_queue_empty(DiffBatchQueue& queue) {
    std::lock_guard<std::mutex> lock(queue.mutex);
    return queue.batches.empty() && queue.error == nullptr;
}

DiffBatchQueueShards make_diff_batch_queue_shards(std::size_t shard_count,
                                                  std::size_t producers,
                                                  std::size_t max_entries_per_shard) {
    DiffBatchQueueShards shards;
    shards.reserve(shard_count);
    for (std::size_t index = 0; index < shard_count; ++index) {
        auto queue = std::make_unique<DiffBatchQueue>();
        configure_diff_batch_queue(*queue, producers, max_entries_per_shard);
        shards.push_back(std::move(queue));
    }
    return shards;
}

std::size_t diff_batch_shard_index(const FlatFolderScanBatch& batch, std::size_t shard_count) {
    if (shard_count == 0U) {
        return 0;
    }
    return std::hash<std::string>{}(normalize_path(batch.folder.rel_path)) % shard_count;
}

bool push_diff_batch(DiffBatchQueueShards& shards, FlatFolderScanBatch batch) {
    if (shards.empty()) {
        return false;
    }
    const std::size_t shard_index = diff_batch_shard_index(batch, shards.size());
    return push_diff_batch(*shards[shard_index], std::move(batch));
}

void finish_diff_batch_producer(DiffBatchQueueShards& shards) {
    for (auto& shard : shards) {
        finish_diff_batch_producer(*shard);
    }
}

void fail_diff_batch_queue(DiffBatchQueueShards& shards,
                           std::exception_ptr error = std::current_exception()) {
    for (auto& shard : shards) {
        fail_diff_batch_queue(*shard, error);
    }
}

void configure_fake_remote_processor_queue(FakeRemoteProcessorQueue& queue,
                                           std::size_t producers,
                                           std::size_t max_entries) {
    std::lock_guard<std::mutex> lock(queue.mutex);
    queue.producers_remaining = producers;
    queue.max_entries = std::max<std::size_t>(1U, max_entries);
    queue.done = producers == 0U;
}

bool push_fake_remote_request(FakeRemoteProcessorQueue& queue, FileSpec folder) {
    std::unique_lock<std::mutex> lock(queue.mutex);
    queue.cv_not_full.wait(lock, [&queue]() {
        return queue.done || queue.error || queue.folders.size() < queue.max_entries;
    });
    if (queue.done || queue.error) {
        queue.cv_not_empty.notify_all();
        queue.cv_not_full.notify_all();
        return false;
    }
    queue.folders.push_back(std::move(folder));
    lock.unlock();
    queue.cv_not_empty.notify_one();
    return true;
}

std::optional<FileSpec> take_fake_remote_request(FakeRemoteProcessorQueue& queue) {
    std::unique_lock<std::mutex> lock(queue.mutex);
    queue.cv_not_empty.wait(lock, [&queue]() {
        return queue.done || queue.error || !queue.folders.empty();
    });
    if (queue.folders.empty()) {
        return std::nullopt;
    }
    FileSpec folder = std::move(queue.folders.front());
    queue.folders.pop_front();
    lock.unlock();
    queue.cv_not_full.notify_one();
    return folder;
}

void finish_fake_remote_request_producer(FakeRemoteProcessorQueue& queue) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        if (queue.producers_remaining != 0U) {
            --queue.producers_remaining;
        }
        if (queue.producers_remaining == 0U) {
            queue.done = true;
        }
    }
    queue.cv_not_empty.notify_all();
    queue.cv_not_full.notify_all();
}

void fail_fake_remote_processor_queue(FakeRemoteProcessorQueue& queue,
                                      std::exception_ptr error = std::current_exception()) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.done = true;
        queue.folders.clear();
        if (!queue.error && error != nullptr) {
            queue.error = error;
        }
    }
    queue.cv_not_empty.notify_all();
    queue.cv_not_full.notify_all();
}

bool all_diff_batch_queues_drained(DiffBatchQueueShards& shards) {
    return std::all_of(shards.begin(), shards.end(), [](const auto& shard) {
        return diff_batch_queue_drained(*shard);
    });
}

bool all_diff_batch_queues_empty(DiffBatchQueueShards& shards) {
    return std::all_of(shards.begin(), shards.end(), [](const auto& shard) {
        return diff_batch_queue_empty(*shard);
    });
}

void add_elapsed_ns(std::atomic<std::uint64_t>& counter,
                    std::chrono::steady_clock::time_point started_at,
                    std::chrono::steady_clock::time_point ended_at) {
    counter.fetch_add(static_cast<std::uint64_t>(
                          std::chrono::duration_cast<std::chrono::nanoseconds>(ended_at - started_at).count()),
                      std::memory_order_relaxed);
}

double ns_to_seconds(std::uint64_t ns) {
    return static_cast<double>(ns) / 1'000'000'000.0;
}

DataReadBenchmarkSnapshot snapshot_data_read_stats(const DataReadBenchmarkStats& stats,
                                                   const ReconScanStats* recon_stats = nullptr) {
    const auto now = std::chrono::steady_clock::now();
    const double elapsed = stats.started_at == std::chrono::steady_clock::time_point{}
                               ? 0.0
                               : std::chrono::duration<double>(now - stats.started_at).count();
    DataReadBenchmarkSnapshot snapshot;
    snapshot.files_found = stats.files_found.load(std::memory_order_relaxed);
    snapshot.folders_found = stats.folders_found.load(std::memory_order_relaxed);
    snapshot.files_read = stats.files_read.load(std::memory_order_relaxed);
    snapshot.files_failed = stats.files_failed.load(std::memory_order_relaxed);
    snapshot.logical_size_bytes = stats.logical_size_bytes.load(std::memory_order_relaxed);
    snapshot.bytes_read = stats.bytes_read.load(std::memory_order_relaxed);
    snapshot.small_files_found = stats.small_files_found.load(std::memory_order_relaxed);
    snapshot.large_files_found = stats.large_files_found.load(std::memory_order_relaxed);
    snapshot.small_files_read = stats.small_files_read.load(std::memory_order_relaxed);
    snapshot.large_files_read = stats.large_files_read.load(std::memory_order_relaxed);
    snapshot.small_bytes_read = stats.small_bytes_read.load(std::memory_order_relaxed);
    snapshot.large_bytes_read = stats.large_bytes_read.load(std::memory_order_relaxed);
    snapshot.small_logical_size_bytes = stats.small_logical_size_bytes.load(std::memory_order_relaxed);
    snapshot.large_logical_size_bytes = stats.large_logical_size_bytes.load(std::memory_order_relaxed);
    snapshot.elapsed_seconds = elapsed;
    snapshot.bytes_per_second = elapsed > 0.0 ? static_cast<double>(snapshot.bytes_read) / elapsed : 0.0;
    snapshot.gigabits_per_second = snapshot.bytes_per_second * 8.0 / 1'000'000'000.0;
    snapshot.files_per_second = elapsed > 0.0 ? static_cast<double>(snapshot.files_read) / elapsed : 0.0;
    snapshot.small_files_per_second =
        elapsed > 0.0 ? static_cast<double>(snapshot.small_files_read) / elapsed : 0.0;
    snapshot.large_files_per_second =
        elapsed > 0.0 ? static_cast<double>(snapshot.large_files_read) / elapsed : 0.0;
    snapshot.small_gigabits_per_second =
        elapsed > 0.0 ? static_cast<double>(snapshot.small_bytes_read) * 8.0 / elapsed / 1'000'000'000.0
                      : 0.0;
    snapshot.large_gigabits_per_second =
        elapsed > 0.0 ? static_cast<double>(snapshot.large_bytes_read) * 8.0 / elapsed / 1'000'000'000.0
                      : 0.0;
    if (recon_stats != nullptr) {
        snapshot.recon_files_found = recon_stats->files_found.load(std::memory_order_relaxed);
        snapshot.recon_folders_found = recon_stats->folders_found.load(std::memory_order_relaxed);
        snapshot.recon_small_files_found = recon_stats->small_files_found.load(std::memory_order_relaxed);
        snapshot.recon_large_files_found = recon_stats->large_files_found.load(std::memory_order_relaxed);
        snapshot.recon_logical_size_bytes = recon_stats->logical_size_bytes.load(std::memory_order_relaxed);
        snapshot.recon_small_logical_size_bytes =
            recon_stats->small_logical_size_bytes.load(std::memory_order_relaxed);
        snapshot.recon_large_logical_size_bytes =
            recon_stats->large_logical_size_bytes.load(std::memory_order_relaxed);
        snapshot.recon_completed = recon_stats->completed.load(std::memory_order_relaxed);
    }
    return snapshot;
}

bool parse_data_copy_mode(std::string_view value) {
    if (value.empty() || value == "copy" || value == "true" || value == "on" || value == "yes") {
        return true;
    }
    if (value == "no-copy" || value == "nocopy" || value == "false" || value == "off" || value == "no") {
        return false;
    }
    throw std::runtime_error("data copy mode must be copy or no-copy");
}

std::string data_copy_mode_name(bool copy_payload_to_buffer) {
    return copy_payload_to_buffer ? "copy" : "no-copy";
}

std::string backend_code_for_source(std::string_view source) {
    if (source.rfind("synthetic-profile://", 0) == 0) {
        return "SYN";
    }
    if (is_nfs_url(source)) {
        return "NFS";
    }
    return "FS";
}

std::string backend_job_name(std::string_view base_name, std::string_view source) {
    return std::string(base_name) + "-" + backend_code_for_source(source);
}

MonitorQueueSnapshot monitor_buf_queue(std::string name, const BufQueue& queue) {
    MonitorQueueSnapshot snapshot;
    snapshot.name = std::move(name);
    snapshot.capacity = queue.capacity();
    snapshot.depth = queue.size();
    snapshot.high_watermark = queue.high_watermark();
    snapshot.pushed = queue.push_count();
    snapshot.popped = queue.pop_count();
    snapshot.closed = queue.closed();
    return snapshot;
}

MonitorQueueSnapshot monitor_flat_folder_queue(std::string name, FlatMetadataWorkQueue& queue) {
    std::lock_guard<std::mutex> lock(queue.mutex);
    MonitorQueueSnapshot snapshot;
    snapshot.name = std::move(name);
    snapshot.capacity = 0;
    snapshot.depth = queue.folders.size();
    snapshot.high_watermark = queue.folders.size();
    snapshot.closed = queue.done;
    snapshot.detail = "active=" + std::to_string(queue.active);
    return snapshot;
}

MonitorQueueSnapshot monitor_data_file_queue(std::string name, DataReadFileQueue& queue) {
    std::lock_guard<std::mutex> lock(queue.mutex);
    MonitorQueueSnapshot snapshot;
    snapshot.name = std::move(name);
    snapshot.capacity = queue.max_entries;
    snapshot.depth = queue.files.size();
    snapshot.high_watermark = queue.files.size();
    snapshot.closed = queue.input_done || queue.stop;
    return snapshot;
}

MonitorQueueSnapshot monitor_queue_group(std::string name,
                                         const std::vector<std::unique_ptr<BufQueue>>& queues) {
    MonitorQueueSnapshot snapshot;
    snapshot.name = std::move(name);
    snapshot.closed = !queues.empty();
    for (const auto& queue : queues) {
        snapshot.capacity += queue->capacity();
        snapshot.depth += queue->size();
        snapshot.high_watermark += queue->high_watermark();
        snapshot.pushed += queue->push_count();
        snapshot.popped += queue->pop_count();
        snapshot.closed = snapshot.closed && queue->closed();
    }
    snapshot.detail = "shards=" + std::to_string(queues.size());
    return snapshot;
}

void add_runtime_metrics(RuntimeMetricsSnapshot& target, const RuntimeMetricsSnapshot& source) {
    target.worker_count += source.worker_count;
    target.total_wall_ns += source.total_wall_ns;
    for (std::size_t index = 0; index < kRuntimeStateCount; ++index) {
        target.state_wall_ns[index] += source.state_wall_ns[index];
        target.current_workers[index] += source.current_workers[index];
    }
}

std::uint64_t file_spec_logical_size(const FileSpec& file) {
    return file.declared_size != 0 ? file.declared_size : file.content.size();
}

bool diff_file_specs_match(const FileSpec& source, const FileSpec& target, const std::string& compare_mode) {
    const std::uint64_t source_size = file_spec_logical_size(source);
    const std::uint64_t target_size = file_spec_logical_size(target);
    if (compare_mode == "size") {
        return source_size == target_size;
    }
    if (compare_mode == "time") {
        return source_size == target_size && source.mtime == target.mtime;
    }
    if (!source.content_hash.empty() && !target.content_hash.empty() &&
        (source.hash_algorithm.empty() || target.hash_algorithm.empty() ||
         source.hash_algorithm == target.hash_algorithm)) {
        return source_size == target_size && source.content_hash == target.content_hash;
    }
    return source_size == target_size && source.mtime == target.mtime;
}

std::string diff_csv_quote(std::string_view value) {
    if (value.find_first_of(",\"\r\n") == std::string_view::npos) {
        return std::string(value);
    }
    std::string quoted = "\"";
    for (char ch : value) {
        if (ch == '"') {
            quoted += "\"\"";
        } else {
            quoted.push_back(ch);
        }
    }
    quoted.push_back('"');
    return quoted;
}

void append_diff_csv_row(std::string& csv,
                         std::string_view rel_path,
                         DiffKind decision,
                         std::uint64_t source_size,
                         std::uint64_t source_mtime,
                         std::uint64_t target_size,
                         std::uint64_t target_mtime) {
    csv += diff_csv_quote(rel_path) + "," + to_string(decision) + "," +
           std::to_string(source_size) + "," + std::to_string(source_mtime) + "," +
           std::to_string(target_size) + "," + std::to_string(target_mtime) + "\n";
}

FolderRecord make_done_folder_record(const FileSpec& folder,
                                      std::uint64_t flat_file_count,
                                      std::uint64_t flat_logical_size_bytes) {
    FolderRecord record;
    record.rel_path = normalize_path(folder.rel_path);
    record.files_discovered = flat_file_count;
    record.files_total = flat_file_count;
    record.files_completed = flat_file_count;
    record.flat_size_bytes = flat_logical_size_bytes;
    record.state = FolderState::done;
    record.remote_state = FolderState::done;
    return record;
}

struct DiffBatchResults {
    std::map<std::string, FileOutcome> files;
    std::map<std::string, FolderRecord> folders;
    std::string csv_rows;
    std::size_t files_total = 0;
    std::size_t folders_total = 0;
    std::size_t files_skipped = 0;
    std::size_t files_changed = 0;
    std::size_t files_new = 0;
    std::size_t files_target_only = 0;
    std::size_t files_failed = 0;
    std::uint64_t bytes_planned = 0;
};

void record_diff_file(DiffBatchResults& results, FileOutcome outcome, bool collect_detailed_records) {
    ++results.files_total;
    switch (outcome.diff) {
        case DiffKind::skip:
            ++results.files_skipped;
            break;
        case DiffKind::new_file:
            ++results.files_new;
            break;
        case DiffKind::changed:
            ++results.files_changed;
            break;
        case DiffKind::target_only:
            ++results.files_target_only;
            break;
        case DiffKind::failed:
            ++results.files_failed;
            break;
    }
    if (collect_detailed_records) {
        results.files[outcome.rel_path] = std::move(outcome);
    }
}

void record_diff_folder(DiffBatchResults& results, FolderRecord record, bool collect_detailed_records) {
    ++results.folders_total;
    if (collect_detailed_records) {
        results.folders[record.rel_path] = std::move(record);
    }
}

void merge_live_diff_results(TransferReport& report,
                             DiffBatchResults&& results,
                             bool collect_detailed_records,
                             std::mutex& report_mutex) {
    std::lock_guard<std::mutex> lock(report_mutex);
    report.files_total += results.files_total;
    report.folders_total += results.folders_total;
    report.files_skipped += results.files_skipped;
    report.files_changed += results.files_changed;
    report.files_new += results.files_new;
    report.files_target_only += results.files_target_only;
    report.files_failed += results.files_failed;
    report.bytes_planned += results.bytes_planned;
    if (collect_detailed_records) {
        report.files.insert(results.files.begin(), results.files.end());
        report.folders.insert(results.folders.begin(), results.folders.end());
        report.diff_csv += results.csv_rows;
    }
}

void add_target_only_tree(NfsBackend& target_backend,
                          const FileSpec& target_folder,
                          FlatMetadataWorkQueue& queue,
                          TransferReport& report,
                          std::mutex& report_mutex,
                          bool collect_detailed_records) {
    if (flat_metadata_scan_should_stop(queue)) {
        return;
    }

    DiffBatchResults results;
    FileSpec normalized_target_folder = target_folder;
    normalized_target_folder.rel_path = normalize_path(normalized_target_folder.rel_path);
    record_diff_folder(results, make_done_folder_record(normalized_target_folder, 0, 0), collect_detailed_records);

    try {
        target_backend.visit_metadata_at(
            target_folder.rel_path,
            true,
            [&](FileSpec file) {
                if (flat_metadata_scan_should_stop(queue)) {
                    return;
                }
                file.rel_path = normalize_path(file.rel_path);
                FileOutcome outcome;
                outcome.rel_path = file.rel_path;
                outcome.diff = DiffKind::target_only;
                outcome.size = file_spec_logical_size(file);
                if (collect_detailed_records) {
                    append_diff_csv_row(results.csv_rows,
                                        outcome.rel_path,
                                        DiffKind::target_only,
                                        0,
                                        0,
                                        outcome.size,
                                        file.mtime);
                }
                record_diff_file(results, std::move(outcome), collect_detailed_records);
            },
            [&](FileSpec directory) {
                if (flat_metadata_scan_should_stop(queue)) {
                    return;
                }
                directory.rel_path = normalize_path(directory.rel_path);
                record_diff_folder(results, make_done_folder_record(directory, 0, 0), collect_detailed_records);
            });
    } catch (const std::exception& ex) {
        FileOutcome outcome;
        outcome.rel_path = normalize_path(target_folder.rel_path);
        outcome.diff = DiffKind::failed;
        const std::string failed_path = outcome.rel_path;
        if (collect_detailed_records) {
            append_diff_csv_row(results.csv_rows, outcome.rel_path, DiffKind::failed, 0, 0, 0, 0);
        }
        record_diff_file(results, std::move(outcome), collect_detailed_records);
        std::cerr << "diff skipped target-only subtree '"
                  << (failed_path.empty() ? "/" : failed_path)
                  << "': " << ex.what() << '\n';
    }

    merge_live_diff_results(report, std::move(results), collect_detailed_records, report_mutex);
}

void record_live_diff_batch(bool recursive,
                            const std::string& compare_mode,
                            FlatMetadataWorkQueue& queue,
                            NfsBackend& target_backend,
                            TransferReport& report,
                            std::mutex& report_mutex,
                            bool collect_detailed_records,
                            FlatFolderScanBatch batch) {
    if (batch.failed) {
        std::cerr << "diff skipped source folder '"
                  << (batch.folder.rel_path.empty() ? "/" : batch.folder.rel_path)
                  << "': " << (batch.error.empty() ? "unknown error" : batch.error) << '\n';
        if (batch.folder.rel_path.empty()) {
            const std::string message = batch.error.empty() ? "failed to scan source root" : batch.error;
            fail_flat_folder_work(queue, std::make_exception_ptr(std::runtime_error(message)));
            return;
        }
        finish_flat_folder_work(queue);
        return;
    }

    const std::string folder_path = normalize_path(batch.folder.rel_path);
    std::uint64_t flat_logical_size = 0;
    std::vector<FileSpec> child_work;
    std::unordered_set<std::string> source_directories;
    if (recursive && !flat_metadata_scan_should_stop(queue)) {
        child_work.reserve(batch.directories.size());
    }
    for (auto& directory : batch.directories) {
        directory.rel_path = normalize_path(directory.rel_path);
        source_directories.insert(directory.rel_path);
        if (recursive && !flat_metadata_scan_should_stop(queue)) {
            child_work.push_back(directory);
        }
    }
    if (flat_metadata_scan_should_stop(queue)) {
        finish_flat_folder_work(queue);
        return;
    }

    std::vector<FileSpec> target_files;
    std::vector<FileSpec> target_directories;
    try {
        target_backend.visit_metadata_at(
            folder_path,
            false,
            [&target_files](FileSpec file) {
                file.rel_path = normalize_path(file.rel_path);
                target_files.push_back(std::move(file));
            },
            [&target_directories](FileSpec directory) {
                directory.rel_path = normalize_path(directory.rel_path);
                target_directories.push_back(std::move(directory));
            });
    } catch (const std::exception& ex) {
        std::cerr << "diff treats target folder '"
                  << (folder_path.empty() ? "/" : folder_path)
                  << "' as empty: " << ex.what() << '\n';
    }

    std::unordered_map<std::string, FileSpec> target_by_path;
    target_by_path.reserve(target_files.size());
    for (auto& file : target_files) {
        target_by_path.emplace(file.rel_path, std::move(file));
    }

    DiffBatchResults results;

    for (auto& source : batch.files) {
        source.rel_path = normalize_path(source.rel_path);
        const std::uint64_t source_size = file_spec_logical_size(source);
        flat_logical_size += source_size;
        FileOutcome outcome;
        outcome.rel_path = source.rel_path;
        outcome.size = source_size;

        const auto target_it = target_by_path.find(source.rel_path);
        if (target_it == target_by_path.end()) {
            outcome.diff = DiffKind::new_file;
            results.bytes_planned += source_size;
            if (collect_detailed_records) {
                append_diff_csv_row(results.csv_rows, source.rel_path, outcome.diff, source_size, source.mtime, 0, 0);
            }
        } else if (diff_file_specs_match(source, target_it->second, compare_mode)) {
            outcome.diff = DiffKind::skip;
            if (collect_detailed_records) {
                append_diff_csv_row(results.csv_rows,
                                    source.rel_path,
                                    outcome.diff,
                                    source_size,
                                    source.mtime,
                                    file_spec_logical_size(target_it->second),
                                    target_it->second.mtime);
            }
        } else {
            outcome.diff = DiffKind::changed;
            results.bytes_planned += source_size;
            if (collect_detailed_records) {
                append_diff_csv_row(results.csv_rows,
                                    source.rel_path,
                                    outcome.diff,
                                    source_size,
                                    source.mtime,
                                    file_spec_logical_size(target_it->second),
                                    target_it->second.mtime);
            }
        }

        record_diff_file(results, std::move(outcome), collect_detailed_records);
        if (target_it != target_by_path.end()) {
            target_by_path.erase(target_it);
        }
    }

    for (const auto& [path, target] : target_by_path) {
        FileOutcome outcome;
        outcome.rel_path = path;
        outcome.diff = DiffKind::target_only;
        outcome.size = file_spec_logical_size(target);
        if (collect_detailed_records) {
            append_diff_csv_row(results.csv_rows, path, outcome.diff, 0, 0, outcome.size, target.mtime);
        }
        record_diff_file(results, std::move(outcome), collect_detailed_records);
    }

    FileSpec normalized_folder = batch.folder;
    normalized_folder.rel_path = folder_path;
    record_diff_folder(results,
                       make_done_folder_record(normalized_folder, batch.files.size(), flat_logical_size),
                       collect_detailed_records);

    merge_live_diff_results(report, std::move(results), collect_detailed_records, report_mutex);

    if (recursive && !flat_metadata_scan_should_stop(queue)) {
        for (const auto& target_directory : target_directories) {
            if (source_directories.find(target_directory.rel_path) == source_directories.end()) {
                add_target_only_tree(target_backend,
                                     target_directory,
                                     queue,
                                     report,
                                     report_mutex,
                                     collect_detailed_records);
            }
        }
        enqueue_flat_folder_work(queue, std::move(child_work));
    }
    finish_flat_folder_work(queue);
}

void live_diff_metadata_worker(const std::string& source_root,
                               const std::string& target_root,
                               bool recursive,
                               const std::string& compare_mode,
                               std::size_t async_directory_depth,
                               FlatMetadataWorkQueue& queue,
                               TransferReport& report,
                               std::mutex& report_mutex,
                               bool collect_detailed_records) {
    auto source_backend = make_nfs_backend(source_root);
    auto target_backend = make_nfs_backend(target_root);
    try {
        source_backend->scan_flat_folders(
            async_directory_depth,
            [&queue](bool wait_for_work) {
                return take_flat_folder_work(queue, wait_for_work);
            },
            [&queue] {
                return flat_metadata_scan_should_stop(queue);
            },
            [recursive,
             &compare_mode,
             &queue,
             &target_backend,
             &report,
             &report_mutex,
             collect_detailed_records](FlatFolderScanBatch batch) {
                record_live_diff_batch(recursive,
                                       compare_mode,
                                       queue,
                                       *target_backend,
                                       report,
                                       report_mutex,
                                       collect_detailed_records,
                                       std::move(batch));
            });
        if (flat_metadata_scan_should_stop(queue)) {
            request_flat_folder_stop(queue);
        }
    } catch (...) {
        fail_flat_folder_work(queue);
    }
}

struct PendingLiveDiffPair {
    std::optional<FlatFolderScanBatch> source;
    std::optional<FlatFolderScanBatch> target;
};

struct LiveDiffSummaryCoordinator {
    std::mutex mutex;
    std::unordered_map<std::string, PendingLiveDiffPair> pending;
};

[[nodiscard]] std::string batch_folder_path(const FlatFolderScanBatch& batch) {
    return normalize_path(batch.folder.rel_path);
}

DiffBatchResults compare_live_diff_summary_batches(const std::string& compare_mode,
                                                   bool recursive,
                                                   bool allow_target_only,
                                                   FlatFolderScanBatch source_batch,
                                                   FlatFolderScanBatch target_batch,
                                                   std::vector<FileSpec>& target_only_child_work) {
    DiffBatchResults results;
    const std::string folder_path = batch_folder_path(source_batch);
    std::uint64_t flat_logical_size = 0;

    std::unordered_set<std::string> source_directories;
    source_directories.reserve(source_batch.directories.size());
    for (auto& directory : source_batch.directories) {
        directory.rel_path = normalize_path(directory.rel_path);
        source_directories.insert(directory.rel_path);
    }

    std::unordered_map<std::string_view, std::size_t> target_by_path;
    std::vector<unsigned char> target_matched;
    if (!target_batch.failed) {
        target_by_path.reserve(target_batch.files.size());
        if (allow_target_only) {
            target_matched.assign(target_batch.files.size(), 0U);
        }
        for (std::size_t index = 0; index < target_batch.files.size(); ++index) {
            auto& file = target_batch.files[index];
            file.rel_path = normalize_path(file.rel_path);
            target_by_path.emplace(std::string_view(file.rel_path), index);
        }
    }

    for (auto& source : source_batch.files) {
        source.rel_path = normalize_path(source.rel_path);
        const std::uint64_t source_size = file_spec_logical_size(source);
        flat_logical_size += source_size;

        FileOutcome outcome;
        outcome.rel_path = source.rel_path;
        outcome.size = source_size;

        const auto target_it = target_by_path.find(std::string_view(source.rel_path));
        if (target_it == target_by_path.end()) {
            outcome.diff = DiffKind::new_file;
            results.bytes_planned += source_size;
        } else if (diff_file_specs_match(source, target_batch.files[target_it->second], compare_mode)) {
            outcome.diff = DiffKind::skip;
        } else {
            outcome.diff = DiffKind::changed;
            results.bytes_planned += source_size;
        }

        record_diff_file(results, std::move(outcome), false);
        if (allow_target_only && target_it != target_by_path.end()) {
            target_matched[target_it->second] = 1U;
        }
    }

    if (allow_target_only) {
        for (std::size_t index = 0; index < target_batch.files.size(); ++index) {
            if (!target_matched.empty() && target_matched[index] != 0U) {
                continue;
            }
            const FileSpec& target = target_batch.files[index];
            FileOutcome outcome;
            outcome.rel_path = target.rel_path;
            outcome.diff = DiffKind::target_only;
            outcome.size = file_spec_logical_size(target);
            record_diff_file(results, std::move(outcome), false);
        }
    }

    FileSpec normalized_folder = source_batch.folder;
    normalized_folder.rel_path = folder_path;
    record_diff_folder(results,
                       make_done_folder_record(normalized_folder, source_batch.files.size(), flat_logical_size),
                       false);

    if (recursive && allow_target_only && !target_batch.failed) {
        for (auto& target_directory : target_batch.directories) {
            target_directory.rel_path = normalize_path(target_directory.rel_path);
            if (source_directories.find(target_directory.rel_path) == source_directories.end()) {
                target_directory.need_check = false;
                target_only_child_work.push_back(std::move(target_directory));
            }
        }
    }

    return results;
}

DiffBatchResults count_target_only_summary_batch(bool recursive,
                                                 FlatFolderScanBatch batch,
                                                 std::vector<FileSpec>& child_work) {
    DiffBatchResults results;
    const std::string folder_path = batch_folder_path(batch);
    if (batch.failed) {
        FileOutcome outcome;
        outcome.rel_path = folder_path;
        outcome.diff = DiffKind::failed;
        record_diff_file(results, std::move(outcome), false);
        return results;
    }

    FileSpec normalized_folder = batch.folder;
    normalized_folder.rel_path = folder_path;
    record_diff_folder(results, make_done_folder_record(normalized_folder, 0, 0), false);

    for (auto& file : batch.files) {
        file.rel_path = normalize_path(file.rel_path);
        FileOutcome outcome;
        outcome.rel_path = file.rel_path;
        outcome.diff = DiffKind::target_only;
        outcome.size = file_spec_logical_size(file);
        record_diff_file(results, std::move(outcome), false);
    }

    if (recursive) {
        child_work.reserve(batch.directories.size());
        for (auto& directory : batch.directories) {
            directory.rel_path = normalize_path(directory.rel_path);
            directory.need_check = false;
            child_work.push_back(std::move(directory));
        }
    }

    return results;
}

void merge_summary_results(TransferReport& report,
                           std::mutex& report_mutex,
                           DiffBatchResults&& results) {
    merge_live_diff_results(report, std::move(results), false, report_mutex);
}

void handle_ready_summary_pair(const std::string& compare_mode,
                               bool recursive,
                               bool allow_target_only,
                               DiffTargetFolderQueue& target_queue,
                               TransferReport& report,
                               std::mutex& report_mutex,
                               FlatFolderScanBatch source_batch,
                               FlatFolderScanBatch target_batch) {
    std::vector<FileSpec> target_only_child_work;
    DiffBatchResults results = compare_live_diff_summary_batches(compare_mode,
                                                                 recursive,
                                                                 allow_target_only,
                                                                 std::move(source_batch),
                                                                 std::move(target_batch),
                                                                 target_only_child_work);
    if (!target_only_child_work.empty()) {
        enqueue_diff_target_folders(target_queue, std::move(target_only_child_work));
    }
    merge_summary_results(report, report_mutex, std::move(results));
}

void store_summary_source_batch(LiveDiffSummaryCoordinator& coordinator,
                                const std::string& compare_mode,
                                bool recursive,
                                bool allow_target_only,
                                DiffTargetFolderQueue& target_queue,
                                TransferReport& report,
                                std::mutex& report_mutex,
                                FlatFolderScanBatch batch) {
    const std::string folder_path = batch_folder_path(batch);
    std::optional<FlatFolderScanBatch> target_batch;
    {
        std::lock_guard<std::mutex> lock(coordinator.mutex);
        PendingLiveDiffPair& pair = coordinator.pending[folder_path];
        pair.source = std::move(batch);
        if (pair.target.has_value()) {
            target_batch = std::move(pair.target);
            batch = std::move(*pair.source);
            coordinator.pending.erase(folder_path);
        }
    }
    if (target_batch.has_value()) {
        handle_ready_summary_pair(compare_mode,
                                  recursive,
                                  allow_target_only,
                                  target_queue,
                                  report,
                                  report_mutex,
                                  std::move(batch),
                                  std::move(*target_batch));
    }
}

void store_summary_target_batch(LiveDiffSummaryCoordinator& coordinator,
                                const std::string& compare_mode,
                                bool recursive,
                                bool allow_target_only,
                                DiffTargetFolderQueue& target_queue,
                                TransferReport& report,
                                std::mutex& report_mutex,
                                FlatFolderScanBatch batch) {
    if (!batch.folder.need_check) {
        std::vector<FileSpec> target_only_child_work;
        DiffBatchResults results = count_target_only_summary_batch(recursive,
                                                                   std::move(batch),
                                                                   target_only_child_work);
        if (!target_only_child_work.empty()) {
            enqueue_diff_target_folders(target_queue, std::move(target_only_child_work));
        }
        merge_summary_results(report, report_mutex, std::move(results));
        return;
    }

    const std::string folder_path = batch_folder_path(batch);
    std::optional<FlatFolderScanBatch> source_batch;
    {
        std::lock_guard<std::mutex> lock(coordinator.mutex);
        PendingLiveDiffPair& pair = coordinator.pending[folder_path];
        pair.target = std::move(batch);
        if (pair.source.has_value()) {
            source_batch = std::move(pair.source);
            batch = std::move(*pair.target);
            coordinator.pending.erase(folder_path);
        }
    }
    if (source_batch.has_value()) {
        handle_ready_summary_pair(compare_mode,
                                  recursive,
                                  allow_target_only,
                                  target_queue,
                                  report,
                                  report_mutex,
                                  std::move(*source_batch),
                                  std::move(batch));
    }
}

void record_source_summary_diff_batch(bool recursive,
                                      FlatMetadataWorkQueue& source_queue,
                                      DiffTargetFolderQueue& target_queue,
                                      DiffBatchQueueShards& source_batches,
                                      FlatFolderScanBatch batch) {
    if (batch.failed) {
        std::cerr << "diff skipped source folder '"
                  << (batch.folder.rel_path.empty() ? "/" : batch.folder.rel_path)
                  << "': " << (batch.error.empty() ? "unknown error" : batch.error) << '\n';
        if (batch.folder.rel_path.empty()) {
            const std::string message = batch.error.empty() ? "failed to scan source root" : batch.error;
            fail_flat_folder_work(source_queue, std::make_exception_ptr(std::runtime_error(message)));
            fail_diff_target_folder_work(target_queue, std::make_exception_ptr(std::runtime_error(message)));
            fail_diff_batch_queue(source_batches, std::make_exception_ptr(std::runtime_error(message)));
            return;
        }
        finish_flat_folder_work(source_queue);
        return;
    }

    const std::string folder_path = batch_folder_path(batch);
    std::vector<FileSpec> child_work;
    if (recursive && !flat_metadata_scan_should_stop(source_queue)) {
        child_work.reserve(batch.directories.size());
        for (auto& directory : batch.directories) {
            directory.rel_path = normalize_path(directory.rel_path);
            child_work.push_back(directory);
        }
    } else {
        for (auto& directory : batch.directories) {
            directory.rel_path = normalize_path(directory.rel_path);
        }
    }

    FileSpec target_folder = batch.folder;
    target_folder.rel_path = folder_path;
    target_folder.need_check = true;
    if (!enqueue_diff_target_folder(target_queue, std::move(target_folder))) {
        finish_flat_folder_work(source_queue);
        return;
    }

    if (!push_diff_batch(source_batches, std::move(batch))) {
        finish_flat_folder_work(source_queue);
        return;
    }
    enqueue_flat_folder_work(source_queue, std::move(child_work));
    finish_flat_folder_work(source_queue);
}

void source_summary_diff_worker(const std::string& source_root,
                                bool recursive,
                                std::size_t async_directory_depth,
                                FlatMetadataWorkQueue& source_queue,
                                DiffTargetFolderQueue& target_queue,
                                DiffBatchQueueShards& source_batches) {
    auto source_backend = make_nfs_backend(source_root);
    try {
        source_backend->scan_flat_folders(
            async_directory_depth,
            [&source_queue](bool wait_for_work) {
                return take_flat_folder_work(source_queue, wait_for_work);
            },
            [&source_queue] {
                return flat_metadata_scan_should_stop(source_queue);
            },
            [recursive,
             &source_queue,
             &target_queue,
             &source_batches](FlatFolderScanBatch batch) {
                record_source_summary_diff_batch(recursive,
                                                 source_queue,
                                                 target_queue,
                                                 source_batches,
                                                 std::move(batch));
            });
        if (flat_metadata_scan_should_stop(source_queue)) {
            request_flat_folder_stop(source_queue);
        }
    } catch (...) {
        fail_flat_folder_work(source_queue);
        fail_diff_target_folder_work(target_queue);
        fail_diff_batch_queue(source_batches);
    }
    finish_diff_batch_producer(source_batches);
}

bool diff_target_queue_should_stop(DiffTargetFolderQueue& queue) {
    std::lock_guard<std::mutex> lock(queue.mutex);
    return queue.done || queue.error != nullptr;
}

void target_summary_diff_worker(const std::string& target_root,
                                std::size_t async_directory_depth,
                                DiffTargetFolderQueue& target_queue,
                                DiffBatchQueueShards& target_batches) {
    auto target_backend = make_nfs_backend(target_root);
    try {
        target_backend->scan_flat_folders(
            async_directory_depth,
            [&target_queue](bool wait_for_work) {
                return take_diff_target_folder_work(target_queue, wait_for_work);
            },
            [&target_queue] {
                return diff_target_queue_should_stop(target_queue);
            },
            [&target_queue, &target_batches](FlatFolderScanBatch batch) {
                if (batch.failed) {
                    std::cerr << "diff treats target folder '"
                              << (batch.folder.rel_path.empty() ? "/" : batch.folder.rel_path)
                              << "' as empty: "
                              << (batch.error.empty() ? "unknown error" : batch.error) << '\n';
                }
                push_diff_batch(target_batches, std::move(batch));
                finish_diff_target_folder_work(target_queue);
            });
    } catch (...) {
        fail_diff_target_folder_work(target_queue);
        fail_diff_batch_queue(target_batches);
    }
    finish_diff_batch_producer(target_batches);
}

void flush_unmatched_summary_batches(LiveDiffSummaryCoordinator& coordinator,
                                     const std::string& compare_mode,
                                     bool recursive,
                                     bool allow_target_only,
                                     DiffTargetFolderQueue& target_queue,
                                     TransferReport& report,
                                     std::mutex& report_mutex);

bool summary_coordinator_empty(LiveDiffSummaryCoordinator& coordinator) {
    std::lock_guard<std::mutex> lock(coordinator.mutex);
    return coordinator.pending.empty();
}

bool all_joiner_pending_empty(const std::vector<std::unique_ptr<std::atomic<bool>>>& pending_empty) {
    return std::all_of(pending_empty.begin(), pending_empty.end(), [](const auto& flag) {
        return flag->load(std::memory_order_relaxed);
    });
}

void close_diff_target_input_when_ready(DiffTargetFolderQueue& target_queue,
                                        DiffBatchQueueShards& source_batches,
                                        DiffBatchQueueShards& target_batches,
                                        const std::vector<std::unique_ptr<std::atomic<bool>>>& pending_empty) {
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(target_queue.mutex);
            if (target_queue.done || target_queue.error) {
                target_queue.cv.notify_all();
                return;
            }
        }
        if (all_diff_batch_queues_drained(source_batches) &&
            all_diff_batch_queues_empty(target_batches) &&
            all_joiner_pending_empty(pending_empty) &&
            diff_target_folder_work_idle(target_queue)) {
            mark_diff_target_input_done(target_queue);
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void summary_diff_joiner_worker(const std::string& compare_mode,
                                bool recursive,
                                bool allow_target_only,
                                DiffTargetFolderQueue& target_queue,
                                DiffBatchQueue& source_batches,
                                DiffBatchQueue& target_batches,
                                TransferReport& report,
                                std::mutex& report_mutex,
                                DiffPipelineTimingCounters* timing = nullptr,
                                std::atomic<bool>* pending_empty = nullptr) {
    LiveDiffSummaryCoordinator coordinator;
    if (pending_empty != nullptr) {
        pending_empty->store(true, std::memory_order_relaxed);
    }
    try {
        for (;;) {
            bool did_work = false;
            while (auto batch = take_diff_batch(source_batches, false)) {
                const auto process_started_at = std::chrono::steady_clock::now();
                if (pending_empty != nullptr) {
                    pending_empty->store(false, std::memory_order_relaxed);
                }
                store_summary_source_batch(coordinator,
                                           compare_mode,
                                           recursive,
                                           allow_target_only,
                                           target_queue,
                                           report,
                                           report_mutex,
                                           std::move(*batch));
                if (timing != nullptr) {
                    add_elapsed_ns(timing->joiner_process_ns,
                                   process_started_at,
                                   std::chrono::steady_clock::now());
                }
                did_work = true;
            }
            while (auto batch = take_diff_batch(target_batches, false)) {
                const auto process_started_at = std::chrono::steady_clock::now();
                if (pending_empty != nullptr) {
                    pending_empty->store(false, std::memory_order_relaxed);
                }
                store_summary_target_batch(coordinator,
                                           compare_mode,
                                           recursive,
                                           allow_target_only,
                                           target_queue,
                                           report,
                                           report_mutex,
                                           std::move(*batch));
                if (timing != nullptr) {
                    add_elapsed_ns(timing->joiner_process_ns,
                                   process_started_at,
                                   std::chrono::steady_clock::now());
                }
                did_work = true;
            }
            if (pending_empty != nullptr) {
                pending_empty->store(summary_coordinator_empty(coordinator), std::memory_order_relaxed);
            }

            if (diff_batch_queue_drained(source_batches) && diff_batch_queue_drained(target_batches)) {
                break;
            }

            if (!did_work) {
                const auto idle_started_at = std::chrono::steady_clock::now();
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                if (timing != nullptr) {
                    add_elapsed_ns(timing->joiner_idle_ns,
                                   idle_started_at,
                                   std::chrono::steady_clock::now());
                }
            }
        }

        flush_unmatched_summary_batches(coordinator,
                                        compare_mode,
                                        recursive,
                                        allow_target_only,
                                        target_queue,
                                        report,
                                        report_mutex);
        if (pending_empty != nullptr) {
            pending_empty->store(true, std::memory_order_relaxed);
        }
    } catch (...) {
        fail_diff_target_folder_work(target_queue);
        fail_diff_batch_queue(source_batches);
        fail_diff_batch_queue(target_batches);
        if (pending_empty != nullptr) {
            pending_empty->store(true, std::memory_order_relaxed);
        }
    }
}

void flush_unmatched_summary_batches(LiveDiffSummaryCoordinator& coordinator,
                                     const std::string& compare_mode,
                                     bool recursive,
                                     bool allow_target_only,
                                     DiffTargetFolderQueue& target_queue,
                                     TransferReport& report,
                                     std::mutex& report_mutex) {
    std::vector<PendingLiveDiffPair> pending;
    {
        std::lock_guard<std::mutex> lock(coordinator.mutex);
        pending.reserve(coordinator.pending.size());
        for (auto& [_, pair] : coordinator.pending) {
            pending.push_back(std::move(pair));
        }
        coordinator.pending.clear();
    }

    for (auto& pair : pending) {
        if (pair.source.has_value()) {
            FlatFolderScanBatch empty_target;
            empty_target.folder = pair.source->folder;
            handle_ready_summary_pair(compare_mode,
                                      recursive,
                                      allow_target_only,
                                      target_queue,
                                      report,
                                      report_mutex,
                                      std::move(*pair.source),
                                      std::move(empty_target));
        } else if (allow_target_only && pair.target.has_value()) {
            std::vector<FileSpec> target_only_child_work;
            DiffBatchResults results = count_target_only_summary_batch(recursive,
                                                                       std::move(*pair.target),
                                                                       target_only_child_work);
            merge_summary_results(report, report_mutex, std::move(results));
        }
    }
}

TransferReport run_summary_live_diff(const std::string& source_root,
                                     const std::string& target_root,
                                     const std::string& compare_mode,
                                     bool recursive,
                                     const NfsMetaReaderConfig& reader_config,
                                     double max_duration_seconds,
                                     std::uint32_t stats_interval_seconds,
                                     std::size_t checker_threads,
                                     std::size_t checker_request_queue_depth,
                                     std::size_t checker_batch_queue_depth) {
    TransferReport report;
    report.mode = Mode::dry_run;
    const auto started_at = std::chrono::steady_clock::now();

    FlatMetadataWorkQueue source_queue;
    source_queue.folders.push_back(FileSpec{});
    if (max_duration_seconds > 0.0) {
        source_queue.stop_at = std::chrono::steady_clock::now() +
                               std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                   std::chrono::duration<double>(max_duration_seconds));
    }

    DiffTargetFolderQueue target_queue;
    std::mutex report_mutex;
    std::atomic<bool> stats_done{false};
    std::thread stats_thread;
    if (stats_interval_seconds != 0U) {
        stats_thread = std::thread([&report, &report_mutex, &stats_done, stats_interval_seconds, started_at]() {
            std::size_t last_records = 0;
            auto last_at = started_at;
            while (!stats_done.load(std::memory_order_relaxed)) {
                std::this_thread::sleep_for(std::chrono::seconds(stats_interval_seconds));
                if (stats_done.load(std::memory_order_relaxed)) {
                    break;
                }
                std::size_t records = 0;
                std::size_t same = 0;
                std::size_t changed = 0;
                std::size_t created = 0;
                std::size_t target_only = 0;
                std::uint64_t bytes_planned = 0;
                {
                    std::lock_guard<std::mutex> lock(report_mutex);
                    records = report.files_total;
                    same = report.files_skipped;
                    changed = report.files_changed;
                    created = report.files_new;
                    target_only = report.files_target_only;
                    bytes_planned = report.bytes_planned;
                }
                const auto now = std::chrono::steady_clock::now();
                const double elapsed = std::chrono::duration<double>(now - started_at).count();
                const double interval_elapsed = std::chrono::duration<double>(now - last_at).count();
                const std::size_t interval_records = records >= last_records ? records - last_records : 0;
                const double average_rate = elapsed > 0.0 ? static_cast<double>(records) / elapsed : 0.0;
                const double interval_rate =
                    interval_elapsed > 0.0 ? static_cast<double>(interval_records) / interval_elapsed : 0.0;
                std::cout << "diff_stats records_per_second=" << average_rate
                          << " interval_records_per_second=" << interval_rate
                          << " diff_records=" << records
                          << " same=" << same
                          << " changed=" << changed
                          << " new=" << created
                          << " target_only=" << target_only
                          << " bytes_planned=" << bytes_planned
                          << " elapsed_seconds=" << elapsed << std::endl;
                last_records = records;
                last_at = now;
            }
        });
    }

    const std::size_t thread_count = std::max<std::size_t>(1, reader_config.worker_count);
    const std::size_t async_depth = std::max<std::size_t>(1, reader_config.async_directory_depth);
    const std::size_t checker_thread_count = std::max<std::size_t>(1, checker_threads);
    const bool allow_target_only = max_duration_seconds <= 0.0;
    const std::size_t derived_request_queue_depth =
        std::max<std::size_t>(16384, thread_count * async_depth * 2U);
    target_queue.max_entries = checker_request_queue_depth != 0U
                                   ? checker_request_queue_depth
                                   : derived_request_queue_depth;
    const std::size_t batch_queue_depth = checker_batch_queue_depth != 0U
                                              ? checker_batch_queue_depth
                                              : std::max<std::size_t>(16384, target_queue.max_entries);
    const std::size_t batch_queue_depth_per_checker =
        std::max<std::size_t>(1024U, (batch_queue_depth + checker_thread_count - 1U) / checker_thread_count);
    DiffBatchQueueShards source_batches =
        make_diff_batch_queue_shards(checker_thread_count, thread_count, batch_queue_depth_per_checker);
    DiffBatchQueueShards target_batches =
        make_diff_batch_queue_shards(checker_thread_count, thread_count, batch_queue_depth_per_checker);
    std::vector<std::unique_ptr<std::atomic<bool>>> joiner_pending_empty;
    joiner_pending_empty.reserve(checker_thread_count);
    for (std::size_t index = 0; index < checker_thread_count; ++index) {
        joiner_pending_empty.push_back(std::make_unique<std::atomic<bool>>(true));
    }
    std::vector<std::thread> target_workers;
    std::vector<std::thread> source_workers;
    std::vector<std::thread> checker_workers;
    target_workers.reserve(thread_count);
    source_workers.reserve(thread_count);
    checker_workers.reserve(checker_thread_count);
    for (std::size_t index = 0; index < thread_count; ++index) {
        target_workers.emplace_back(target_summary_diff_worker,
                                    target_root,
                                    async_depth,
                                    std::ref(target_queue),
                                    std::ref(target_batches));
    }
    for (std::size_t index = 0; index < checker_thread_count; ++index) {
        checker_workers.emplace_back(summary_diff_joiner_worker,
                                     compare_mode,
                                     recursive,
                                     allow_target_only,
                                     std::ref(target_queue),
                                     std::ref(*source_batches[index]),
                                     std::ref(*target_batches[index]),
                                     std::ref(report),
                                     std::ref(report_mutex),
                                     nullptr,
                                     joiner_pending_empty[index].get());
    }
    std::thread target_closer(close_diff_target_input_when_ready,
                              std::ref(target_queue),
                              std::ref(source_batches),
                              std::ref(target_batches),
                              std::cref(joiner_pending_empty));
    for (std::size_t index = 0; index < thread_count; ++index) {
        source_workers.emplace_back(source_summary_diff_worker,
                                    source_root,
                                    recursive,
                                    async_depth,
                                    std::ref(source_queue),
                                    std::ref(target_queue),
                                    std::ref(source_batches));
    }

    for (auto& worker : source_workers) {
        worker.join();
    }
    if (target_closer.joinable()) {
        target_closer.join();
    }
    for (auto& worker : target_workers) {
        worker.join();
    }
    for (auto& worker : checker_workers) {
        worker.join();
    }
    stats_done.store(true, std::memory_order_relaxed);
    if (stats_thread.joinable()) {
        stats_thread.join();
    }

    if (source_queue.error) {
        std::rethrow_exception(source_queue.error);
    }
    if (target_queue.error) {
        std::rethrow_exception(target_queue.error);
    }
    for (const auto& queue : source_batches) {
        if (queue->error) {
            std::rethrow_exception(queue->error);
        }
    }
    for (const auto& queue : target_batches) {
        if (queue->error) {
            std::rethrow_exception(queue->error);
        }
    }
    return report;
}

enum class DistributedDiffFrameType : std::uint32_t {
    config = 1,
    source_folder = 2,
    source_done = 3,
    folder_result = 4,
    target_done = 5,
    error = 6,
    source_manifest = 7,
    bulk_summary = 8,
};

struct DistributedDiffFrame {
    DistributedDiffFrameType type = DistributedDiffFrameType::error;
    std::string payload;
};

struct DistributedDiffSettings {
    std::string compare_mode = "size";
    bool recursive = true;
    bool allow_target_only = true;
};

struct DistributedFolderDiffSummary {
    std::string rel_path;
    std::uint64_t source_scan_started_unix_ns = 0;
    std::uint64_t source_scan_finished_unix_ns = 0;
    std::uint64_t target_scan_started_unix_ns = 0;
    std::uint64_t target_scan_finished_unix_ns = 0;
    std::uint64_t result_sent_unix_ns = 0;
    std::uint64_t result_received_unix_ns = 0;
    std::uint64_t source_file_count = 0;
    std::uint64_t target_file_count = 0;
    std::uint64_t source_folder_count = 0;
    std::uint64_t target_folder_count = 0;
    std::uint64_t files_same = 0;
    std::uint64_t files_changed = 0;
    std::uint64_t files_new = 0;
    std::uint64_t files_target_only = 0;
    std::uint64_t files_failed = 0;
    std::uint64_t source_logical_size_bytes = 0;
    std::uint64_t target_logical_size_bytes = 0;
    std::uint64_t same_logical_size_bytes = 0;
    std::uint64_t changed_logical_size_bytes = 0;
    std::uint64_t new_logical_size_bytes = 0;
    std::uint64_t target_only_logical_size_bytes = 0;
    std::uint64_t bytes_planned = 0;
    std::uint8_t status = 0;
    std::string error;
};

struct DistributedTargetWork {
    std::optional<FlatFolderScanBatch> source_batch;
    FileSpec target_folder;
    bool target_only = false;
};

struct DistributedTargetQueue {
    std::mutex mutex;
    std::condition_variable cv;
    std::deque<DistributedTargetWork> work;
    std::size_t active = 0;
    bool input_done = false;
    bool done = false;
    std::exception_ptr error;
};

struct DistributedDiffSharedStats {
    std::mutex mutex;
    DistributedDiffRunReport report;
};

inline constexpr std::uint64_t kDistributedDiffFrameMagic = 0x4859444946463031ULL;  // HYDIFF01.
inline constexpr std::uint32_t kDistributedDiffFrameVersion = 1U;
inline constexpr std::size_t kDistributedDiffHeaderBytes = 24U;
inline constexpr std::size_t kDistributedDiffMaxPayloadBytes = 512U * 1024U * 1024U;

std::uint64_t now_unix_ns() {
    return unix_time_nanoseconds(std::chrono::system_clock::now());
}

std::string current_exception_message() {
    try {
        throw;
    } catch (const std::exception& ex) {
        return ex.what();
    } catch (...) {
        return "unknown error";
    }
}

void append_u8(std::string& out, std::uint8_t value) {
    out.push_back(static_cast<char>(value));
}

void append_u32_be(std::string& out, std::uint32_t value) {
    for (int shift = 24; shift >= 0; shift -= 8) {
        out.push_back(static_cast<char>((value >> static_cast<unsigned>(shift)) & 0xFFU));
    }
}

void append_u64_be(std::string& out, std::uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        out.push_back(static_cast<char>((value >> static_cast<unsigned>(shift)) & 0xFFU));
    }
}

void append_string_field(std::string& out, std::string_view value) {
    if (value.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::overflow_error("distributed diff string field is too large");
    }
    append_u32_be(out, static_cast<std::uint32_t>(value.size()));
    out.append(value.data(), value.size());
}

std::uint8_t read_u8_field(std::string_view input, std::size_t& offset) {
    if (offset >= input.size()) {
        throw std::runtime_error("distributed diff payload is truncated");
    }
    return static_cast<std::uint8_t>(input[offset++]);
}

std::uint32_t read_u32_be_field(std::string_view input, std::size_t& offset) {
    if (input.size() - offset < sizeof(std::uint32_t)) {
        throw std::runtime_error("distributed diff payload is truncated");
    }
    std::uint32_t value = 0;
    for (int index = 0; index < 4; ++index) {
        value = (value << 8U) | static_cast<unsigned char>(input[offset++]);
    }
    return value;
}

std::uint64_t read_u64_be_field(std::string_view input, std::size_t& offset) {
    if (input.size() - offset < sizeof(std::uint64_t)) {
        throw std::runtime_error("distributed diff payload is truncated");
    }
    std::uint64_t value = 0;
    for (int index = 0; index < 8; ++index) {
        value = (value << 8U) | static_cast<unsigned char>(input[offset++]);
    }
    return value;
}

std::string read_string_field(std::string_view input, std::size_t& offset) {
    const std::uint32_t bytes = read_u32_be_field(input, offset);
    if (input.size() - offset < bytes) {
        throw std::runtime_error("distributed diff string field is truncated");
    }
    std::string value(input.substr(offset, bytes));
    offset += bytes;
    return value;
}

std::string join_rel_child_path(std::string_view folder_path, std::string_view child_name) {
    const std::string normalized_folder = normalize_path(folder_path);
    if (normalized_folder.empty()) {
        return normalize_path(child_name);
    }
    return normalize_path(normalized_folder + "/" + std::string(child_name));
}

std::string child_name_for_folder(std::string_view folder_path, std::string_view rel_path) {
    const std::string normalized_folder = normalize_path(folder_path);
    const std::string normalized_path = normalize_path(rel_path);
    if (!normalized_folder.empty() &&
        normalized_path.size() > normalized_folder.size() &&
        normalized_path.compare(0U, normalized_folder.size(), normalized_folder) == 0 &&
        normalized_path[normalized_folder.size()] == '/') {
        return normalized_path.substr(normalized_folder.size() + 1U);
    }
    return base_name(normalized_path);
}

[[maybe_unused]] void write_distributed_diff_frame(int fd, DistributedDiffFrameType type, std::string_view payload) {
    if (payload.size() > kDistributedDiffMaxPayloadBytes) {
        throw std::runtime_error("distributed diff frame exceeds payload limit");
    }
    std::string header;
    header.reserve(kDistributedDiffHeaderBytes);
    append_u64_be(header, kDistributedDiffFrameMagic);
    append_u32_be(header, kDistributedDiffFrameVersion);
    append_u32_be(header, static_cast<std::uint32_t>(type));
    append_u64_be(header, static_cast<std::uint64_t>(payload.size()));
    write_all(fd, header.data(), header.size());
    if (!payload.empty()) {
        write_all(fd, payload.data(), payload.size());
    }
}

[[maybe_unused]] std::optional<DistributedDiffFrame> read_distributed_diff_frame(int fd) {
    std::array<char, kDistributedDiffHeaderBytes> header {};
    if (!read_exact_or_eof(fd, header.data(), header.size())) {
        return std::nullopt;
    }
    std::string_view header_view(header.data(), header.size());
    std::size_t offset = 0;
    if (read_u64_be_field(header_view, offset) != kDistributedDiffFrameMagic) {
        throw std::runtime_error("distributed diff frame magic is invalid");
    }
    if (read_u32_be_field(header_view, offset) != kDistributedDiffFrameVersion) {
        throw std::runtime_error("distributed diff frame version is unsupported");
    }
    DistributedDiffFrame frame;
    frame.type = static_cast<DistributedDiffFrameType>(read_u32_be_field(header_view, offset));
    const std::uint64_t payload_bytes = read_u64_be_field(header_view, offset);
    if (payload_bytes > kDistributedDiffMaxPayloadBytes) {
        throw std::runtime_error("distributed diff frame payload is too large");
    }
    frame.payload.assign(static_cast<std::size_t>(payload_bytes), '\0');
    if (payload_bytes != 0U &&
        !read_exact_or_eof(fd, frame.payload.data(), static_cast<std::size_t>(payload_bytes))) {
        throw std::runtime_error("distributed diff frame payload ended early");
    }
    return frame;
}

[[maybe_unused]] std::string serialize_distributed_diff_settings(const DistributedDiffSettings& settings) {
    std::string payload;
    append_string_field(payload, settings.compare_mode);
    append_u8(payload, settings.recursive ? 1U : 0U);
    append_u8(payload, settings.allow_target_only ? 1U : 0U);
    return payload;
}

[[maybe_unused]] DistributedDiffSettings deserialize_distributed_diff_settings(std::string_view payload) {
    std::size_t offset = 0;
    DistributedDiffSettings settings;
    settings.compare_mode = read_string_field(payload, offset);
    settings.recursive = read_u8_field(payload, offset) != 0U;
    settings.allow_target_only = read_u8_field(payload, offset) != 0U;
    return settings;
}

inline constexpr std::size_t kBulkManifestPayloadBytes = 1024U * 1024U;
inline constexpr std::size_t kBulkManifestTokenBytes = sizeof(std::uint64_t) * 3U;

struct BulkManifestToken {
    std::uint64_t path_hash = 0;
    std::uint64_t size = 0;
    std::uint64_t mtime = 0;
};

struct BulkTargetState {
    std::uint64_t size = 0;
    std::uint64_t mtime = 0;
    bool seen = false;
};

struct BulkTargetStateShard {
    std::mutex mutex;
    std::unordered_map<std::uint64_t, BulkTargetState> entries;
};

struct BulkTargetStateMap {
    std::vector<std::unique_ptr<BulkTargetStateShard>> shards;
    std::atomic<std::uint64_t> files {0};
    std::atomic<std::uint64_t> logical_size_bytes {0};

    explicit BulkTargetStateMap(std::size_t shard_count = 256U) {
        const std::size_t count = std::max<std::size_t>(1U, shard_count);
        shards.reserve(count);
        for (std::size_t index = 0; index < count; ++index) {
            shards.push_back(std::make_unique<BulkTargetStateShard>());
        }
    }

    [[nodiscard]] BulkTargetStateShard& shard_for(std::uint64_t path_hash) const {
        return *shards[static_cast<std::size_t>(path_hash % shards.size())];
    }

    void insert(const FileSpec& file) {
        const std::uint64_t size = file_spec_logical_size(file);
        const std::uint64_t path_hash = hash64(normalize_path(file.rel_path));
        BulkTargetStateShard& shard = shard_for(path_hash);
        {
            std::lock_guard<std::mutex> lock(shard.mutex);
            shard.entries[path_hash] = BulkTargetState{size, file.mtime, false};
        }
        files.fetch_add(1U, std::memory_order_relaxed);
        logical_size_bytes.fetch_add(size, std::memory_order_relaxed);
    }

    [[nodiscard]] std::optional<BulkTargetState> mark_seen(std::uint64_t path_hash) const {
        BulkTargetStateShard& shard = shard_for(path_hash);
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.entries.find(path_hash);
        if (it == shard.entries.end()) {
            return std::nullopt;
        }
        it->second.seen = true;
        return it->second;
    }

    [[nodiscard]] std::pair<std::uint64_t, std::uint64_t> target_only_counts() const {
        std::uint64_t count = 0;
        std::uint64_t bytes = 0;
        for (const auto& shard_ptr : shards) {
            std::lock_guard<std::mutex> lock(shard_ptr->mutex);
            for (const auto& [_, entry] : shard_ptr->entries) {
                if (!entry.seen) {
                    ++count;
                    bytes += entry.size;
                }
            }
        }
        return {count, bytes};
    }
};

void append_bulk_manifest_token(std::string& payload, const FileSpec& file) {
    append_u64_be(payload, hash64(normalize_path(file.rel_path)));
    append_u64_be(payload, file_spec_logical_size(file));
    append_u64_be(payload, file.mtime);
}

BulkManifestToken read_bulk_manifest_token(std::string_view payload, std::size_t& offset) {
    BulkManifestToken token;
    token.path_hash = read_u64_be_field(payload, offset);
    token.size = read_u64_be_field(payload, offset);
    token.mtime = read_u64_be_field(payload, offset);
    return token;
}

bool bulk_manifest_token_matches(const BulkManifestToken& source,
                                 const BulkTargetState& target,
                                 const std::string& compare_mode) {
    if (compare_mode == "size") {
        return source.size == target.size;
    }
    if (compare_mode == "time" || compare_mode == "mtime") {
        return source.size == target.size && source.mtime == target.mtime;
    }
    if (compare_mode == "content") {
        return source.size == target.size && source.mtime == target.mtime;
    }
    return source.size == target.size && source.mtime == target.mtime;
}

void mark_data_file_input_done(DataReadFileQueue& queue);
void scan_data_read_metadata_worker(const std::string& source_root,
                                    bool recursive,
                                    std::size_t async_directory_depth,
                                    std::size_t readdirplus_page_bytes,
                                    std::uint64_t min_file_size_bytes,
                                    std::uint64_t max_file_size_bytes,
                                    FlatMetadataWorkQueue& folder_queue,
                                    DataReadFileQueue& file_queue,
                                    DataReadBenchmarkStats& stats,
                                    RawBufferPool* target_metadata_pool,
                                    BufQueue* target_metadata_queue);

void scan_metadata_to_file_queue(const std::string& root,
                                 bool recursive,
                                 std::size_t worker_count,
                                 std::size_t async_depth,
                                 std::size_t readdirplus_page_bytes,
                                 DataReadFileQueue& file_queue,
                                 DataReadBenchmarkStats& stats,
                                 std::optional<std::chrono::steady_clock::time_point> stop_at = std::nullopt) {
    FlatMetadataWorkQueue folder_queue;
    folder_queue.folders.push_back(FileSpec{});
    folder_queue.stop_at = stop_at;
    file_queue.stop_at = stop_at;
    stats.folders_found.store(1U, std::memory_order_relaxed);

    std::vector<std::thread> workers;
    workers.reserve(worker_count);
    for (std::size_t index = 0; index < worker_count; ++index) {
        workers.emplace_back(scan_data_read_metadata_worker,
                             root,
                             recursive,
                             std::max<std::size_t>(1U, async_depth),
                             readdirplus_page_bytes,
                             0,
                             0,
                             std::ref(folder_queue),
                             std::ref(file_queue),
                             std::ref(stats),
                             nullptr,
                             nullptr);
    }
    for (std::thread& worker : workers) {
        worker.join();
    }
    mark_data_file_input_done(file_queue);
    if (folder_queue.error) {
        std::rethrow_exception(folder_queue.error);
    }
}

void append_file_spec_compact(std::string& payload, const FileSpec& spec, std::string_view folder_path) {
    append_string_field(payload, child_name_for_folder(folder_path, spec.rel_path));
    append_u64_be(payload, file_spec_logical_size(spec));
    append_u64_be(payload, spec.mtime);
    append_u32_be(payload, spec.mode);
    append_u32_be(payload, spec.uid);
    append_u32_be(payload, spec.gid);
}

FileSpec read_file_spec_compact(std::string_view payload, std::size_t& offset, std::string_view folder_path) {
    FileSpec spec;
    spec.rel_path = join_rel_child_path(folder_path, read_string_field(payload, offset));
    spec.declared_size = read_u64_be_field(payload, offset);
    spec.mtime = read_u64_be_field(payload, offset);
    spec.mode = read_u32_be_field(payload, offset);
    spec.uid = read_u32_be_field(payload, offset);
    spec.gid = read_u32_be_field(payload, offset);
    return spec;
}

[[maybe_unused]] std::string serialize_flat_folder_scan_batch(const FlatFolderScanBatch& batch) {
    const std::string folder_path = normalize_path(batch.folder.rel_path);
    std::string payload;
    append_string_field(payload, folder_path);
    append_u64_be(payload, batch.scan_started_unix_ns);
    append_u64_be(payload, batch.scan_finished_unix_ns);
    append_u8(payload, batch.failed ? 1U : 0U);
    append_string_field(payload, batch.error);
    append_u64_be(payload, batch.files.size());
    append_u64_be(payload, batch.directories.size());
    for (const auto& file : batch.files) {
        append_file_spec_compact(payload, file, folder_path);
    }
    for (const auto& directory : batch.directories) {
        append_file_spec_compact(payload, directory, folder_path);
    }
    return payload;
}

[[maybe_unused]] FlatFolderScanBatch deserialize_flat_folder_scan_batch(std::string_view payload) {
    std::size_t offset = 0;
    FlatFolderScanBatch batch;
    batch.folder.rel_path = normalize_path(read_string_field(payload, offset));
    batch.scan_started_unix_ns = read_u64_be_field(payload, offset);
    batch.scan_finished_unix_ns = read_u64_be_field(payload, offset);
    batch.failed = read_u8_field(payload, offset) != 0U;
    batch.error = read_string_field(payload, offset);
    const std::uint64_t file_count = read_u64_be_field(payload, offset);
    const std::uint64_t directory_count = read_u64_be_field(payload, offset);
    if (file_count > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max()) ||
        directory_count > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
        throw std::runtime_error("distributed diff folder batch has too many children");
    }
    batch.files.reserve(static_cast<std::size_t>(file_count));
    for (std::uint64_t index = 0; index < file_count; ++index) {
        batch.files.push_back(read_file_spec_compact(payload, offset, batch.folder.rel_path));
    }
    batch.directories.reserve(static_cast<std::size_t>(directory_count));
    for (std::uint64_t index = 0; index < directory_count; ++index) {
        batch.directories.push_back(read_file_spec_compact(payload, offset, batch.folder.rel_path));
    }
    if (offset != payload.size()) {
        throw std::runtime_error("distributed diff folder batch has trailing bytes");
    }
    return batch;
}

[[maybe_unused]] std::string serialize_folder_diff_summary(const DistributedFolderDiffSummary& summary) {
    std::string payload;
    append_string_field(payload, summary.rel_path);
    append_u64_be(payload, summary.source_scan_started_unix_ns);
    append_u64_be(payload, summary.source_scan_finished_unix_ns);
    append_u64_be(payload, summary.target_scan_started_unix_ns);
    append_u64_be(payload, summary.target_scan_finished_unix_ns);
    append_u64_be(payload, summary.result_sent_unix_ns);
    append_u64_be(payload, summary.source_file_count);
    append_u64_be(payload, summary.target_file_count);
    append_u64_be(payload, summary.source_folder_count);
    append_u64_be(payload, summary.target_folder_count);
    append_u64_be(payload, summary.files_same);
    append_u64_be(payload, summary.files_changed);
    append_u64_be(payload, summary.files_new);
    append_u64_be(payload, summary.files_target_only);
    append_u64_be(payload, summary.files_failed);
    append_u64_be(payload, summary.source_logical_size_bytes);
    append_u64_be(payload, summary.target_logical_size_bytes);
    append_u64_be(payload, summary.same_logical_size_bytes);
    append_u64_be(payload, summary.changed_logical_size_bytes);
    append_u64_be(payload, summary.new_logical_size_bytes);
    append_u64_be(payload, summary.target_only_logical_size_bytes);
    append_u64_be(payload, summary.bytes_planned);
    append_u8(payload, summary.status);
    append_string_field(payload, summary.error);
    return payload;
}

[[maybe_unused]] DistributedFolderDiffSummary deserialize_folder_diff_summary(std::string_view payload) {
    std::size_t offset = 0;
    DistributedFolderDiffSummary summary;
    summary.rel_path = normalize_path(read_string_field(payload, offset));
    summary.source_scan_started_unix_ns = read_u64_be_field(payload, offset);
    summary.source_scan_finished_unix_ns = read_u64_be_field(payload, offset);
    summary.target_scan_started_unix_ns = read_u64_be_field(payload, offset);
    summary.target_scan_finished_unix_ns = read_u64_be_field(payload, offset);
    summary.result_sent_unix_ns = read_u64_be_field(payload, offset);
    summary.source_file_count = read_u64_be_field(payload, offset);
    summary.target_file_count = read_u64_be_field(payload, offset);
    summary.source_folder_count = read_u64_be_field(payload, offset);
    summary.target_folder_count = read_u64_be_field(payload, offset);
    summary.files_same = read_u64_be_field(payload, offset);
    summary.files_changed = read_u64_be_field(payload, offset);
    summary.files_new = read_u64_be_field(payload, offset);
    summary.files_target_only = read_u64_be_field(payload, offset);
    summary.files_failed = read_u64_be_field(payload, offset);
    summary.source_logical_size_bytes = read_u64_be_field(payload, offset);
    summary.target_logical_size_bytes = read_u64_be_field(payload, offset);
    summary.same_logical_size_bytes = read_u64_be_field(payload, offset);
    summary.changed_logical_size_bytes = read_u64_be_field(payload, offset);
    summary.new_logical_size_bytes = read_u64_be_field(payload, offset);
    summary.target_only_logical_size_bytes = read_u64_be_field(payload, offset);
    summary.bytes_planned = read_u64_be_field(payload, offset);
    summary.status = read_u8_field(payload, offset);
    summary.error = read_string_field(payload, offset);
    if (offset != payload.size()) {
        throw std::runtime_error("distributed diff summary has trailing bytes");
    }
    return summary;
}

[[maybe_unused]] DistributedFolderDiffSummary compare_distributed_folder_batches(const std::string& compare_mode,
                                                                bool recursive,
                                                                bool allow_target_only,
                                                                const FlatFolderScanBatch& source_batch,
                                                                const FlatFolderScanBatch& target_batch,
                                                                std::vector<FileSpec>& target_only_child_work) {
    DistributedFolderDiffSummary summary;
    summary.rel_path = batch_folder_path(source_batch);
    summary.source_scan_started_unix_ns = source_batch.scan_started_unix_ns;
    summary.source_scan_finished_unix_ns = source_batch.scan_finished_unix_ns;
    summary.target_scan_started_unix_ns = target_batch.scan_started_unix_ns;
    summary.target_scan_finished_unix_ns = target_batch.scan_finished_unix_ns;
    summary.source_file_count = source_batch.files.size();
    summary.target_file_count = target_batch.failed ? 0U : target_batch.files.size();
    summary.source_folder_count = source_batch.directories.size();
    summary.target_folder_count = target_batch.failed ? 0U : target_batch.directories.size();

    if (source_batch.failed) {
        summary.status = 1U;
        summary.files_failed = 1U;
        summary.error = source_batch.error;
        return summary;
    }
    if (target_batch.failed) {
        summary.status = 2U;
        summary.error = target_batch.error;
    }

    std::unordered_set<std::string> source_directories;
    source_directories.reserve(source_batch.directories.size());
    for (const auto& directory : source_batch.directories) {
        source_directories.insert(normalize_path(directory.rel_path));
    }

    std::unordered_map<std::string_view, std::size_t> target_by_path;
    std::vector<unsigned char> target_matched;
    if (!target_batch.failed) {
        target_by_path.reserve(target_batch.files.size());
        target_matched.assign(target_batch.files.size(), 0U);
        for (std::size_t index = 0; index < target_batch.files.size(); ++index) {
            target_by_path.emplace(std::string_view(target_batch.files[index].rel_path), index);
            summary.target_logical_size_bytes += file_spec_logical_size(target_batch.files[index]);
        }
    }

    for (const auto& source : source_batch.files) {
        const std::uint64_t source_size = file_spec_logical_size(source);
        summary.source_logical_size_bytes += source_size;
        const auto target_it = target_by_path.find(std::string_view(source.rel_path));
        if (target_it == target_by_path.end()) {
            ++summary.files_new;
            summary.new_logical_size_bytes += source_size;
            summary.bytes_planned += source_size;
            continue;
        }
        target_matched[target_it->second] = 1U;
        if (diff_file_specs_match(source, target_batch.files[target_it->second], compare_mode)) {
            ++summary.files_same;
            summary.same_logical_size_bytes += source_size;
        } else {
            ++summary.files_changed;
            summary.changed_logical_size_bytes += source_size;
            summary.bytes_planned += source_size;
        }
    }

    if (allow_target_only && !target_batch.failed) {
        for (std::size_t index = 0; index < target_batch.files.size(); ++index) {
            if (target_matched[index] != 0U) {
                continue;
            }
            ++summary.files_target_only;
            summary.target_only_logical_size_bytes += file_spec_logical_size(target_batch.files[index]);
        }
        if (recursive) {
            for (const auto& target_directory : target_batch.directories) {
                if (source_directories.find(normalize_path(target_directory.rel_path)) == source_directories.end()) {
                    FileSpec child = target_directory;
                    child.need_check = false;
                    target_only_child_work.push_back(std::move(child));
                }
            }
        }
    }

    return summary;
}

[[maybe_unused]] DistributedFolderDiffSummary summarize_target_only_folder_batch(bool recursive,
                                                                const FlatFolderScanBatch& target_batch,
                                                                std::vector<FileSpec>& target_only_child_work) {
    DistributedFolderDiffSummary summary;
    summary.rel_path = batch_folder_path(target_batch);
    summary.target_scan_started_unix_ns = target_batch.scan_started_unix_ns;
    summary.target_scan_finished_unix_ns = target_batch.scan_finished_unix_ns;
    summary.target_file_count = target_batch.failed ? 0U : target_batch.files.size();
    summary.target_folder_count = target_batch.failed ? 0U : target_batch.directories.size();
    if (target_batch.failed) {
        summary.status = 2U;
        summary.files_failed = 1U;
        summary.error = target_batch.error;
        return summary;
    }
    for (const auto& file : target_batch.files) {
        ++summary.files_target_only;
        const std::uint64_t size = file_spec_logical_size(file);
        summary.target_logical_size_bytes += size;
        summary.target_only_logical_size_bytes += size;
    }
    if (recursive) {
        for (const auto& directory : target_batch.directories) {
            FileSpec child = directory;
            child.need_check = false;
            target_only_child_work.push_back(std::move(child));
        }
    }
    return summary;
}

[[maybe_unused]] void enqueue_distributed_target_work(DistributedTargetQueue& queue, DistributedTargetWork work) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        if (queue.done || queue.error) {
            return;
        }
        queue.work.push_back(std::move(work));
    }
    queue.cv.notify_one();
}

[[maybe_unused]] std::optional<DistributedTargetWork> take_distributed_target_work(DistributedTargetQueue& queue) {
    std::unique_lock<std::mutex> lock(queue.mutex);
    queue.cv.wait(lock, [&queue]() {
        return queue.done || queue.error || !queue.work.empty() || queue.input_done;
    });
    if (queue.error || queue.done || queue.work.empty()) {
        return std::nullopt;
    }
    DistributedTargetWork work = std::move(queue.work.front());
    queue.work.pop_front();
    ++queue.active;
    return work;
}

[[maybe_unused]] void finish_distributed_target_work(DistributedTargetQueue& queue) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        if (queue.active != 0U) {
            --queue.active;
        }
        if (queue.input_done && queue.work.empty() && queue.active == 0U) {
            queue.done = true;
        }
    }
    queue.cv.notify_all();
}

[[maybe_unused]] void mark_distributed_target_input_done(DistributedTargetQueue& queue) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.input_done = true;
        if (queue.work.empty() && queue.active == 0U) {
            queue.done = true;
        }
    }
    queue.cv.notify_all();
}

[[maybe_unused]] void fail_distributed_target_queue(DistributedTargetQueue& queue,
                                   std::exception_ptr error = std::current_exception()) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.done = true;
        queue.work.clear();
        if (!queue.error && error != nullptr) {
            queue.error = error;
        }
    }
    queue.cv.notify_all();
}

void write_folder_diff_csv_header(std::ostream& out) {
    out << "rel_path,"
        << "source_scan_started_unix_ns,source_scan_finished_unix_ns,"
        << "target_scan_started_unix_ns,target_scan_finished_unix_ns,"
        << "result_sent_unix_ns,result_received_unix_ns,"
        << "source_file_count,target_file_count,total_file_count,"
        << "source_folder_count,target_folder_count,"
        << "same_file_count,changed_file_count,new_file_count,target_only_file_count,failed_file_count,"
        << "source_logical_size_bytes,target_logical_size_bytes,"
        << "same_logical_size_bytes,changed_logical_size_bytes,new_logical_size_bytes,target_only_logical_size_bytes,"
        << "bytes_planned,status,error\n";
}

[[maybe_unused]] void write_folder_diff_csv_row(std::ostream& out, const DistributedFolderDiffSummary& summary) {
    out << diff_csv_quote(summary.rel_path) << ','
        << summary.source_scan_started_unix_ns << ','
        << summary.source_scan_finished_unix_ns << ','
        << summary.target_scan_started_unix_ns << ','
        << summary.target_scan_finished_unix_ns << ','
        << summary.result_sent_unix_ns << ','
        << summary.result_received_unix_ns << ','
        << summary.source_file_count << ','
        << summary.target_file_count << ','
        << (summary.source_file_count + summary.target_file_count) << ','
        << summary.source_folder_count << ','
        << summary.target_folder_count << ','
        << summary.files_same << ','
        << summary.files_changed << ','
        << summary.files_new << ','
        << summary.files_target_only << ','
        << summary.files_failed << ','
        << summary.source_logical_size_bytes << ','
        << summary.target_logical_size_bytes << ','
        << summary.same_logical_size_bytes << ','
        << summary.changed_logical_size_bytes << ','
        << summary.new_logical_size_bytes << ','
        << summary.target_only_logical_size_bytes << ','
        << summary.bytes_planned << ','
        << static_cast<unsigned>(summary.status) << ','
        << diff_csv_quote(summary.error) << '\n';
}

[[maybe_unused]] void merge_distributed_diff_summary(DistributedDiffRunReport& report,
                                    const DistributedFolderDiffSummary& summary) {
    ++report.folders_reported;
    report.files_compared += summary.source_file_count + summary.files_target_only;
    report.files_same += summary.files_same;
    report.files_changed += summary.files_changed;
    report.files_new += summary.files_new;
    report.files_target_only += summary.files_target_only;
    report.files_failed += summary.files_failed;
    report.source_logical_size_bytes += summary.source_logical_size_bytes;
    report.target_logical_size_bytes += summary.target_logical_size_bytes;
    report.bytes_planned += summary.bytes_planned;
}

void print_distributed_diff_source_stats(const DistributedDiffRunReport& report,
                                         std::chrono::steady_clock::time_point started_at,
                                         std::uint64_t last_folders,
                                         std::uint64_t last_files,
                                         std::chrono::steady_clock::time_point last_at) {
    const auto now = std::chrono::steady_clock::now();
    const double elapsed = std::chrono::duration<double>(now - started_at).count();
    const double interval_elapsed = std::chrono::duration<double>(now - last_at).count();
    const std::uint64_t interval_folders =
        report.folders_reported >= last_folders ? report.folders_reported - last_folders : 0U;
    const std::uint64_t interval_files =
        report.files_compared >= last_files ? report.files_compared - last_files : 0U;
    const double folders_per_second = elapsed > 0.0 ? static_cast<double>(report.folders_reported) / elapsed : 0.0;
    const double interval_folders_per_second =
        interval_elapsed > 0.0 ? static_cast<double>(interval_folders) / interval_elapsed : 0.0;
    const double files_per_second = elapsed > 0.0 ? static_cast<double>(report.files_compared) / elapsed : 0.0;
    const double interval_files_per_second =
        interval_elapsed > 0.0 ? static_cast<double>(interval_files) / interval_elapsed : 0.0;
    std::cout << "distributed_diff_source_stats"
              << " folders_sent=" << report.folders_sent
              << " folders_reported=" << report.folders_reported
              << " folders_per_second=" << folders_per_second
              << " interval_folders_per_second=" << interval_folders_per_second
              << " files_per_second=" << files_per_second
              << " interval_files_per_second=" << interval_files_per_second
              << " files_compared=" << report.files_compared
              << " same=" << report.files_same
              << " changed=" << report.files_changed
              << " new=" << report.files_new
              << " target_only=" << report.files_target_only
              << " bytes_planned=" << report.bytes_planned
              << " elapsed_seconds=" << elapsed << std::endl;
}

std::size_t metadata_batch_payload_size(RawBufferPool& pool, const BufferHandle& handle) {
    const MetadataBatchBuffer& buffer = metadata_batch_buffer(pool, handle);
    return sizeof(std::uint32_t) + sizeof(std::uint32_t) + buffer.bytes_used;
}

void merge_folder_diff_summary(DistributedDiffRunReport& report, const FolderDiffSummary& summary) {
    ++report.folders_reported;
    report.files_compared += summary.source_file_count + summary.files_target_only;
    report.files_same += summary.files_same;
    report.files_changed += summary.files_changed;
    report.files_new += summary.files_new;
    report.files_target_only += summary.files_target_only;
    report.files_failed += summary.files_failed;
    report.source_logical_size_bytes += summary.source_logical_size_bytes;
    report.target_logical_size_bytes += summary.target_logical_size_bytes;
    report.bytes_planned += summary.bytes_planned;
}

void write_folder_diff_csv_row(std::ostream& out, const FolderDiffSummary& summary) {
    out << diff_csv_quote(summary.rel_path) << ','
        << summary.source_scan_started_unix_ns << ','
        << summary.source_scan_finished_unix_ns << ','
        << summary.target_scan_started_unix_ns << ','
        << summary.target_scan_finished_unix_ns << ','
        << summary.result_sent_unix_ns << ','
        << summary.result_received_unix_ns << ','
        << summary.source_file_count << ','
        << summary.target_file_count << ','
        << (summary.source_file_count + summary.target_file_count) << ','
        << summary.source_folder_count << ','
        << summary.target_folder_count << ','
        << summary.files_same << ','
        << summary.files_changed << ','
        << summary.files_new << ','
        << summary.files_target_only << ','
        << summary.files_failed << ','
        << summary.source_logical_size_bytes << ','
        << summary.target_logical_size_bytes << ','
        << summary.same_logical_size_bytes << ','
        << summary.changed_logical_size_bytes << ','
        << summary.new_logical_size_bytes << ','
        << summary.target_only_logical_size_bytes << ','
        << summary.bytes_planned << ','
        << static_cast<unsigned>(summary.status) << ','
        << diff_csv_quote(summary.error) << '\n';
}

template <typename AcquireBuffer, typename PushBuffer>
void pack_flat_folder_batch_to_queue_with(const FlatFolderScanBatch& batch,
                                          std::string_view compare_mode,
                                          RawBufferPool& pool,
                                          AcquireBuffer acquire_buffer,
                                          PushBuffer push_buffer) {
    const std::uint64_t logical_size = flat_folder_logical_size(batch.files);
    const std::uint64_t metadata_hash =
        batch.failed ? 0U : flat_folder_metadata_hash(batch.files, batch.directories, compare_mode);
    const std::uint64_t file_count = batch.failed ? 0U : static_cast<std::uint64_t>(batch.files.size());
    const std::uint64_t folder_count = batch.failed ? 0U : static_cast<std::uint64_t>(batch.directories.size());
    std::uint32_t sequence = 0;

    BufferHandle current = acquire_buffer();
    reset_flat_folder_buffer(metadata_batch_buffer(pool, current),
                             batch.folder,
                             sequence,
                             false,
                             batch.failed,
                             batch.error,
                             file_count,
                             folder_count,
                             logical_size,
                             metadata_hash,
                             batch.scan_started_unix_ns,
                             batch.scan_finished_unix_ns);

    if (batch.failed) {
        set_flat_folder_buffer_final(metadata_batch_buffer(pool, current), true);
        push_buffer(current);
        return;
    }

    const auto flush_current = [&](bool final_batch) {
        set_flat_folder_buffer_final(metadata_batch_buffer(pool, current), final_batch);
        push_buffer(current);
    };

    const auto start_next = [&] {
        ++sequence;
        current = acquire_buffer();
        reset_flat_folder_buffer(metadata_batch_buffer(pool, current),
                                 batch.folder,
                                 sequence,
                                 false,
                                 false,
                                 {},
                                 file_count,
                                 folder_count,
                                 logical_size,
                                 metadata_hash,
                                 batch.scan_started_unix_ns,
                                 batch.scan_finished_unix_ns);
    };

    for (const FileSpec& file : batch.files) {
        if (!append_flat_folder_file(metadata_batch_buffer(pool, current), file, compare_mode)) {
            flush_current(false);
            start_next();
            if (!append_flat_folder_file(metadata_batch_buffer(pool, current), file, compare_mode)) {
                pool.release(current);
                throw std::runtime_error("single file record is too large for flat-folder metadata buffer");
            }
        }
    }
    for (const FileSpec& folder : batch.directories) {
        if (!append_flat_folder_folder(metadata_batch_buffer(pool, current), folder, compare_mode)) {
            flush_current(false);
            start_next();
            if (!append_flat_folder_folder(metadata_batch_buffer(pool, current), folder, compare_mode)) {
                pool.release(current);
                throw std::runtime_error("single folder record is too large for flat-folder metadata buffer");
            }
        }
    }
    flush_current(true);
}

inline constexpr BufferPoolId kFolderWorkBufferPoolId = 34;
inline constexpr BufferPoolId kFolderFeedbackBufferPoolId = 35;

enum class FolderWorkKind : std::uint32_t {
    folder = 1,
    complete = 2,
    error = 3,
};

void reset_folder_work_buffer(MetadataBuffer& buffer,
                              FolderWorkKind kind,
                              std::string_view rel_path,
                              bool recursive) {
    if (rel_path.size() > buffer.bytes.size()) {
        throw std::runtime_error("folder work path does not fit metadata buffer");
    }
    buffer.record_kind = kind == FolderWorkKind::folder ? MetadataBufferRecordKind::folder
                       : kind == FolderWorkKind::complete ? MetadataBufferRecordKind::folder_complete
                                                          : MetadataBufferRecordKind::error;
    buffer.bytes_used = static_cast<std::uint32_t>(rel_path.size());
    buffer.file_id = static_cast<std::uint64_t>(kind);
    buffer.folder_hash = recursive ? 1U : 0U;
    if (!rel_path.empty()) {
        std::memcpy(buffer.bytes.data(), rel_path.data(), rel_path.size());
    }
}

FolderWorkKind folder_work_kind(const MetadataBuffer& buffer) {
    if (buffer.record_kind == MetadataBufferRecordKind::folder) {
        return FolderWorkKind::folder;
    }
    if (buffer.record_kind == MetadataBufferRecordKind::folder_complete) {
        return FolderWorkKind::complete;
    }
    if (buffer.record_kind == MetadataBufferRecordKind::error) {
        return FolderWorkKind::error;
    }
    throw std::runtime_error("invalid folder work buffer kind");
}

FileSpec folder_spec_from_work_buffer(const MetadataBuffer& buffer) {
    FileSpec folder;
    folder.rel_path = std::string(reinterpret_cast<const char*>(buffer.bytes.data()), buffer.bytes_used);
    folder.recursive = buffer.folder_hash != 0U;
    return folder;
}

RawBufferPool make_folder_work_buffer_pool(BufferPoolId pool_id, std::size_t capacity) {
    return RawBufferPool(pool_id, capacity, sizeof(MetadataBuffer), alignof(MetadataBuffer));
}

std::size_t folder_work_slots(std::size_t worker_count, std::size_t async_depth) {
    return std::max<std::size_t>(64U, worker_count * std::max<std::size_t>(1U, async_depth) * 4U);
}

class FolderSeederJob final : public ThreadedJob {
public:
    FolderSeederJob(bool recursive,
                    double max_duration_seconds,
                    RawBufferPool& folder_pool,
                    RawBufferPool& feedback_pool,
                    BufQueue& folder_output,
                    BufQueue& feedback_input)
        : ThreadedJob(1U),
          recursive_(recursive),
          max_duration_seconds_(max_duration_seconds),
          folder_pool_(folder_pool),
          feedback_pool_(feedback_pool),
          folder_output_(folder_output),
          feedback_input_(feedback_input) {}

    [[nodiscard]] bool should_stop() const {
        return stop_requested() || expired();
    }

    [[nodiscard]] std::exception_ptr error() const {
        std::lock_guard<std::mutex> lock(error_mutex_);
        return error_;
    }

protected:
    void on_starting() override {
        if (max_duration_seconds_ > 0.0) {
            stop_at_ = std::chrono::steady_clock::now() +
                       std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                           std::chrono::duration<double>(max_duration_seconds_));
        } else {
            stop_at_.reset();
        }
        active_folders_ = 0;
        error_ = nullptr;
    }

    void run_worker(std::size_t worker_index) override {
        std::deque<FileSpec> pending_folders;
        FileSpec root;
        root.recursive = recursive_;
        pending_folders.push_back(std::move(root));
        active_folders_ = 1U;

        BufferHandle feedback;
        while (active_folders_ != 0U && !stop_requested() && !expired()) {
            bool progressed = false;
            std::size_t emitted = 0;
            while (!pending_folders.empty() && emitted < 4096U) {
                const FolderEmitResult emit_result = try_emit_folder(worker_index, pending_folders.front());
                if (emit_result == FolderEmitResult::emitted) {
                    pending_folders.pop_front();
                    progressed = true;
                    ++emitted;
                    continue;
                }
                if (emit_result == FolderEmitResult::stopped) {
                    pending_folders.clear();
                }
                break;
            }
            std::size_t feedback_drained = 0;
            while (feedback_drained < 4096U && feedback_input_.try_pop(feedback)) {
                ++feedback_drained;
                progressed = true;
                MetadataBuffer& buffer = metadata_buffer(feedback_pool_, feedback);
                const FolderWorkKind kind = folder_work_kind(buffer);
                if (kind == FolderWorkKind::folder) {
                    FileSpec folder = folder_spec_from_work_buffer(buffer);
                    feedback_pool_.release(feedback);
                    pending_folders.push_back(std::move(folder));
                    ++active_folders_;
                } else if (kind == FolderWorkKind::complete) {
                    feedback_pool_.release(feedback);
                    if (active_folders_ != 0U) {
                        --active_folders_;
                    }
                } else {
                    const std::string message(reinterpret_cast<const char*>(buffer.bytes.data()), buffer.bytes_used);
                    feedback_pool_.release(feedback);
                    {
                        std::lock_guard<std::mutex> lock(error_mutex_);
                        error_ = std::make_exception_ptr(std::runtime_error(message));
                    }
                    pending_folders.clear();
                    break;
                }
            }
            if (!progressed) {
                {
                    auto wait_scope = runtime_state_scope(worker_index, RuntimeState::wait_input_empty);
                    std::this_thread::sleep_for(std::chrono::microseconds(100));
                }
            }
        }
        folder_output_.close();
        feedback_input_.close();
    }

    void on_stop_requested() override {
        folder_output_.close();
        feedback_input_.close();
    }

private:
    enum class FolderEmitResult {
        emitted,
        blocked,
        stopped,
    };

    [[nodiscard]] bool expired() const {
        return stop_at_.has_value() && std::chrono::steady_clock::now() >= *stop_at_;
    }

    FolderEmitResult try_emit_folder(std::size_t worker_index, const FileSpec& folder) {
        if (stop_requested() || expired()) {
            return FolderEmitResult::stopped;
        }
        std::optional<BufferHandle> maybe_handle = folder_pool_.try_acquire();
        if (!maybe_handle.has_value()) {
            auto wait_scope = runtime_state_scope(worker_index, RuntimeState::wait_pool_empty);
            return FolderEmitResult::blocked;
        }
        const BufferHandle handle = *maybe_handle;
        reset_folder_work_buffer(metadata_buffer(folder_pool_, handle),
                                 FolderWorkKind::folder,
                                 folder.rel_path,
                                 folder.recursive);
        if (!folder_output_.try_push(handle)) {
            folder_pool_.release(handle);
            auto wait_scope = runtime_state_scope(worker_index, RuntimeState::wait_output_full);
            return FolderEmitResult::blocked;
        }
        (void)worker_index;
        return FolderEmitResult::emitted;
    }

    bool recursive_ = true;
    double max_duration_seconds_ = 0.0;
    RawBufferPool& folder_pool_;
    RawBufferPool& feedback_pool_;
    BufQueue& folder_output_;
    BufQueue& feedback_input_;
    std::optional<std::chrono::steady_clock::time_point> stop_at_;
    std::size_t active_folders_ = 0;
    mutable std::mutex error_mutex_;
    std::exception_ptr error_;
};

class NfsMetaReaderBufferJob final : public ThreadedJob {
public:
    NfsMetaReaderBufferJob(std::string root,
                           std::string compare_mode,
                           std::size_t worker_count,
                           std::size_t async_depth,
                           RawBufferPool& folder_pool,
                           RawBufferPool& feedback_pool,
                           BufQueue& folder_input,
                           BufQueue& folder_feedback,
                           RawBufferPool& output_pool,
                           BufQueue& output,
                           double max_duration_seconds)
        : ThreadedJob(worker_count),
          root_(std::move(root)),
          compare_mode_(std::move(compare_mode)),
          async_depth_(std::max<std::size_t>(1U, async_depth)),
          folder_pool_(folder_pool),
          feedback_pool_(feedback_pool),
          folder_input_(folder_input),
          folder_feedback_(folder_feedback),
          output_pool_(output_pool),
          output_(&output),
          max_duration_seconds_(max_duration_seconds) {}

    NfsMetaReaderBufferJob(std::string root,
                           std::string compare_mode,
                           std::size_t worker_count,
                           std::size_t async_depth,
                           RawBufferPool& folder_pool,
                           RawBufferPool& feedback_pool,
                           BufQueue& folder_input,
                           BufQueue& folder_feedback,
                           RawBufferPool& output_pool,
                           ShardedBufQueue& output,
                           double max_duration_seconds)
        : ThreadedJob(worker_count),
          root_(std::move(root)),
          compare_mode_(std::move(compare_mode)),
          async_depth_(std::max<std::size_t>(1U, async_depth)),
          folder_pool_(folder_pool),
          feedback_pool_(feedback_pool),
          folder_input_(folder_input),
          folder_feedback_(folder_feedback),
          output_pool_(output_pool),
          sharded_output_(&output),
          max_duration_seconds_(max_duration_seconds) {}

    NfsMetaReaderBufferJob(std::string root,
                           std::string compare_mode,
                           std::size_t worker_count,
                           std::size_t async_depth,
                           RawBufferPool& folder_pool,
                           RawBufferPool& feedback_pool,
                           BufQueue& folder_input,
                           BufQueue& folder_feedback,
                           RawBufferPool& output_pool,
                           std::vector<BufQueue*> output_queues,
                           double max_duration_seconds)
        : ThreadedJob(worker_count),
          root_(std::move(root)),
          compare_mode_(std::move(compare_mode)),
          async_depth_(std::max<std::size_t>(1U, async_depth)),
          folder_pool_(folder_pool),
          feedback_pool_(feedback_pool),
          folder_input_(folder_input),
          folder_feedback_(folder_feedback),
          output_pool_(output_pool),
          output_queues_(std::move(output_queues)),
          max_duration_seconds_(max_duration_seconds) {
        if (output_queues_.empty()) {
            throw std::invalid_argument("flat folder scanner requires at least one output queue");
        }
    }

    [[nodiscard]] DistributedDiffRunReport stats() const {
        DistributedDiffRunReport report;
        report.folders_sent = folders_sent_.load(std::memory_order_acquire);
        report.files_compared = files_seen_.load(std::memory_order_acquire);
        report.source_logical_size_bytes = logical_size_.load(std::memory_order_acquire);
        return report;
    }

protected:
    void on_starting() override {
        if (max_duration_seconds_ > 0.0) {
            stop_at_ = std::chrono::steady_clock::now() +
                       std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                           std::chrono::duration<double>(max_duration_seconds_));
        } else {
            stop_at_.reset();
        }
    }

    void run_worker(std::size_t worker_index) override {
        auto backend = make_nfs_backend(root_);
        auto io_scope = runtime_state_scope(worker_index, RuntimeState::wait_io);
        backend->scan_flat_folders(
            async_depth_,
            [this, worker_index](bool wait_for_work) -> std::optional<FileSpec> {
                if (!wait_until_worker_active(worker_index)) {
                    return std::optional<FileSpec> {};
                }
                BufferHandle handle;
                const bool got_work = wait_for_work ? wait_for_input(worker_index, folder_input_, handle)
                                                    : folder_input_.try_pop(handle);
                if (!got_work) {
                    return std::optional<FileSpec> {};
                }
                const FileSpec folder = folder_spec_from_work_buffer(metadata_buffer(folder_pool_, handle));
                folder_pool_.release(handle);
                return folder;
            },
            [this] {
                return stop_requested() || expired() || folder_input_.closed();
            },
            [this, worker_index](FlatFolderScanBatch batch) {
                if (batch.failed) {
                    std::cerr << "metadata scan skipped folder '"
                              << (batch.folder.rel_path.empty() ? "/" : batch.folder.rel_path)
                              << "': " << (batch.error.empty() ? "unknown error" : batch.error) << '\n';
                    if (batch.folder.rel_path.empty()) {
                        const std::string message =
                            batch.error.empty() ? "failed to scan root metadata folder" : batch.error;
                        send_feedback(worker_index, FolderWorkKind::error, message, false);
                    } else {
                        send_feedback(worker_index, FolderWorkKind::complete, {}, false);
                    }
                    return;
                }
                if (batch.folder.recursive) {
                    for (FileSpec directory : batch.directories) {
                        directory.rel_path = normalize_path(directory.rel_path);
                        send_feedback(worker_index, FolderWorkKind::folder, directory.rel_path, batch.folder.recursive);
                    }
                }
                files_seen_.fetch_add(batch.files.size(), std::memory_order_relaxed);
                logical_size_.fetch_add(flat_folder_logical_size(batch.files), std::memory_order_relaxed);
                pack_flat_folder_batch_to_queue_with(
                    batch,
                    compare_mode_,
                    output_pool_,
                    [this, worker_index] {
                        if (auto handle = wait_for_pool(worker_index, output_pool_); handle.has_value()) {
                            return *handle;
                        }
                        throw std::runtime_error("flat folder scanner stopped while waiting for output buffer");
                    },
                    [this, worker_index](const BufferHandle& handle) {
                        bool pushed = false;
                        if (!output_queues_.empty()) {
                            const MetadataBatchBuffer& buffer = metadata_batch_buffer(output_pool_, handle);
                            const FlatFolderBufferInfo info = flat_folder_buffer_info(buffer);
                            BufQueue& queue =
                                *output_queues_[metadata_partition_for_flat_folder_buffer(info, output_queues_.size())];
                            pushed = wait_for_output(worker_index, queue, handle);
                        } else {
                            pushed = sharded_output_ != nullptr
                                         ? wait_for_output(worker_index, *sharded_output_, handle)
                                         : wait_for_output(worker_index, *output_, handle);
                        }
                        if (!pushed) {
                            output_pool_.release(handle);
                            throw std::runtime_error("flat folder scanner output queue closed");
                        }
                    });
                folders_sent_.fetch_add(1U, std::memory_order_relaxed);
                send_feedback(worker_index, FolderWorkKind::complete, {}, false);
            });
    }

    void on_stop_requested() override {
        folder_input_.close();
        folder_feedback_.close();
        close_output();
    }

    void on_all_workers_finished() override {
        close_output();
    }

private:
    [[nodiscard]] bool expired() const {
        return stop_at_.has_value() && std::chrono::steady_clock::now() >= *stop_at_;
    }

    void send_feedback(std::size_t worker_index,
                       FolderWorkKind kind,
                       std::string_view rel_path,
                       bool recursive) {
        std::optional<BufferHandle> maybe_handle = wait_for_pool(worker_index, feedback_pool_);
        if (!maybe_handle.has_value()) {
            return;
        }
        const BufferHandle handle = *maybe_handle;
        reset_folder_work_buffer(metadata_buffer(feedback_pool_, handle), kind, rel_path, recursive);
        if (!wait_for_output(worker_index, folder_feedback_, handle)) {
            feedback_pool_.release(handle);
        }
    }

    void close_output() {
        if (!output_queues_.empty()) {
            for (BufQueue* queue : output_queues_) {
                queue->close();
            }
            return;
        }
        if (sharded_output_ != nullptr) {
            sharded_output_->close();
            return;
        }
        if (output_ != nullptr) {
            output_->close();
        }
    }

    std::string root_;
    std::string compare_mode_;
    std::size_t async_depth_ = 1;
    RawBufferPool& folder_pool_;
    RawBufferPool& feedback_pool_;
    BufQueue& folder_input_;
    BufQueue& folder_feedback_;
    RawBufferPool& output_pool_;
    BufQueue* output_ = nullptr;
    ShardedBufQueue* sharded_output_ = nullptr;
    std::vector<BufQueue*> output_queues_;
    double max_duration_seconds_ = 0.0;
    std::optional<std::chrono::steady_clock::time_point> stop_at_;
    std::atomic<std::uint64_t> folders_sent_ {0};
    std::atomic<std::uint64_t> files_seen_ {0};
    std::atomic<std::uint64_t> logical_size_ {0};
};

std::string child_path_for_flat_folder(std::string_view folder_path, std::string_view child_name) {
    if (folder_path.empty()) {
        return std::string(child_name);
    }
    std::string path;
    path.reserve(folder_path.size() + 1U + child_name.size());
    path.append(folder_path);
    path.push_back('/');
    path.append(child_name);
    return path;
}

class FlatFolderMetadataConsumerJob final : public BufferConsumerJob {
public:
    FlatFolderMetadataConsumerJob(std::size_t worker_count,
                                  BufQueue& input,
                                  const BufferPoolRegistry& registry,
                                  MetadataStatsDiscarder* stats_discarder,
                                  MetadataRecordWriter* record_writer,
                                  PartitionedMetadataWriter* partitioned_writer)
        : BufferConsumerJob(std::max<std::size_t>(1U, worker_count), input, registry),
          stats_discarder_(stats_discarder),
          record_writer_(record_writer),
          partitioned_writer_(partitioned_writer) {}

protected:
    void process_buffer(const BufferHandle& handle, RawBufferPool& pool) override {
        if (handle.pool_id != kMetadataBatchBufferPoolId) {
            pool.release(handle);
            throw std::runtime_error("flat metadata consumer received non-metadata buffer");
        }

        const MetadataBatchBuffer& buffer = metadata_batch_buffer(pool, handle);
        if (!is_flat_folder_buffer(buffer)) {
            pool.release(handle);
            throw std::runtime_error("flat metadata consumer received non-flat-folder metadata buffer");
        }

        const FlatFolderBufferInfo info = flat_folder_buffer_info(buffer);
        std::size_t files_found = 0;
        std::size_t folders_found = 0;
        std::uint64_t logical_size_bytes = 0;
        std::vector<FileSpec> files;
        std::vector<MetadataFolderRecord> folder_records;
        const bool write_records = record_writer_ != nullptr || partitioned_writer_ != nullptr;
        if (write_records) {
            files.reserve(info.child_record_count);
        }

        visit_flat_folder_children(buffer, [&](FlatFolderChildView child) {
            if (child.is_file) {
                ++files_found;
                logical_size_bytes += child.logical_size;
                if (write_records) {
                    FileSpec file;
                    file.rel_path = child_path_for_flat_folder(info.folder_path, child.name);
                    file.declared_size = child.logical_size;
                    file.mtime = child.mtime;
                    file.mode = child.mode;
                    file.uid = child.uid;
                    file.gid = child.gid;
                    files.push_back(std::move(file));
                }
            } else {
                ++folders_found;
            }
        });

        if (stats_discarder_ != nullptr) {
            stats_discarder_->record_batch(files_found, logical_size_bytes, folders_found);
        }

        if (write_records && info.final_batch) {
            MetadataFolderRecord folder_record;
            folder_record.spec.rel_path = std::string(info.folder_path);
            folder_record.spec.mode = info.folder_mode;
            folder_record.spec.uid = info.folder_uid;
            folder_record.spec.gid = info.folder_gid;
            folder_record.flat_file_count = static_cast<std::size_t>(info.total_file_count);
            folder_record.flat_logical_size_bytes = info.total_logical_size_bytes;
            folder_records.push_back(std::move(folder_record));
        }

        if (record_writer_ != nullptr && (!files.empty() || !folder_records.empty())) {
            record_writer_->write_batch(files, folder_records);
        } else if (partitioned_writer_ != nullptr && (!files.empty() || !folder_records.empty())) {
            MetadataFolderRecord folder_record;
            if (!folder_records.empty()) {
                folder_record = std::move(folder_records.front());
            } else {
                folder_record.spec.rel_path = std::string(info.folder_path);
                folder_record.flat_file_count = static_cast<std::size_t>(info.total_file_count);
                folder_record.flat_logical_size_bytes = info.total_logical_size_bytes;
            }
            partitioned_writer_->write_batch(files, folder_record);
        }

        pool.release(handle);
    }

private:
    MetadataStatsDiscarder* stats_discarder_ = nullptr;
    MetadataRecordWriter* record_writer_ = nullptr;
    PartitionedMetadataWriter* partitioned_writer_ = nullptr;
};

class PartitionedFlatFolderMetadataRouterJob final : public BufferConsumerJob {
public:
    PartitionedFlatFolderMetadataRouterJob(std::size_t worker_count,
                                           BufQueue& input,
                                           const BufferPoolRegistry& registry,
                                           MetadataStatsDiscarder* stats_discarder,
                                           PartitionedMetadataWriter& partitioned_writer)
        : BufferConsumerJob(std::max<std::size_t>(1U, worker_count), input, registry),
          stats_discarder_(stats_discarder),
          partitioned_writer_(&partitioned_writer) {}

    PartitionedFlatFolderMetadataRouterJob(std::size_t worker_count,
                                           BufQueue& input,
                                           const BufferPoolRegistry& registry,
                                           MetadataStatsDiscarder* stats_discarder,
                                           std::vector<std::unique_ptr<BufQueue>>& partition_queues)
        : BufferConsumerJob(std::max<std::size_t>(1U, worker_count), input, registry),
          stats_discarder_(stats_discarder),
          partition_queues_(&partition_queues) {
        if (partition_queues.empty()) {
            throw std::invalid_argument("metadata route discard requires at least one partition queue");
        }
    }

protected:
    void process_buffer(const BufferHandle& handle, RawBufferPool& pool) override {
        if (handle.pool_id != kMetadataBatchBufferPoolId) {
            pool.release(handle);
            throw std::runtime_error("metadata router received non-metadata buffer");
        }

        const MetadataBatchBuffer& buffer = metadata_batch_buffer(pool, handle);
        if (!is_flat_folder_buffer(buffer)) {
            pool.release(handle);
            throw std::runtime_error("metadata router received non-flat-folder metadata buffer");
        }

        if (stats_discarder_ != nullptr) {
            std::size_t files_found = 0;
            std::size_t folders_found = 0;
            std::uint64_t logical_size_bytes = 0;
            visit_flat_folder_children(buffer, [&](FlatFolderChildView child) {
                if (child.is_file) {
                    ++files_found;
                    logical_size_bytes += child.logical_size;
                } else {
                    ++folders_found;
                }
            });
            stats_discarder_->record_batch(files_found, logical_size_bytes, folders_found);
        }

        if (partitioned_writer_ != nullptr) {
            try {
                partitioned_writer_->route_flat_folder_buffer(handle, pool);
            } catch (...) {
                pool.release(handle);
                throw;
            }
            return;
        }

        if (partition_queues_ == nullptr || partition_queues_->empty()) {
            pool.release(handle);
            throw std::runtime_error("metadata router has no output route");
        }
        const FlatFolderBufferInfo info = flat_folder_buffer_info(buffer);
        const std::size_t partition =
            metadata_partition_for_flat_folder_buffer(info, partition_queues_->size());
        if (!(*partition_queues_)[partition]->push_wait(handle)) {
            pool.release(handle);
            throw std::runtime_error("metadata route discard queue closed while pushing scan buffer");
        }
    }

private:
    MetadataStatsDiscarder* stats_discarder_ = nullptr;
    PartitionedMetadataWriter* partitioned_writer_ = nullptr;
    std::vector<std::unique_ptr<BufQueue>>* partition_queues_ = nullptr;
};

std::size_t shard_for_folder_hash(std::uint64_t folder_hash, std::size_t shard_count) {
    return shard_count == 0U ? 0U : static_cast<std::size_t>(folder_hash % shard_count);
}

std::uint64_t packed_small_parent_locality_hash(const DataBuffer& buffer, std::uint64_t fallback) {
    std::uint64_t key = fallback;
    bool found = false;
    const bool ok = visit_packed_small_files(buffer, [&](PackedSmallFileView view) {
        if (found) {
            return;
        }
        if (view.folder_hash != 0U) {
            key = view.folder_hash;
            found = true;
            return;
        }
        const std::string_view path = view.rel_path;
        const std::size_t slash = path.find_last_of('/');
        if (slash != std::string_view::npos && slash != 0U) {
            key = hash64(path.substr(0U, slash));
        } else if (slash == 0U) {
            key = hash64(std::string_view {path.data(), 1U});
        }
        found = true;
    });
    return ok && found ? key : fallback;
}

inline constexpr BufferPoolId kDistributedDiffSourcePoolId = 30;
inline constexpr BufferPoolId kDistributedDiffRequestPoolId = 31;
inline constexpr BufferPoolId kDistributedDiffTargetPoolId = 32;
inline constexpr BufferPoolId kDistributedDiffResultPoolId = 33;
inline constexpr std::uint32_t kDistributedDiffResultFlushRecords = 512;
inline constexpr auto kDistributedDiffResultFlushInterval = std::chrono::milliseconds(500);
inline constexpr std::size_t kDistributedDiffFlatBufferSlotCap = 8192;
inline constexpr std::size_t kDistributedDiffControlBufferSlotCap = 1024;

RawBufferPool make_distributed_diff_metadata_pool(BufferPoolId pool_id, std::size_t capacity) {
    return RawBufferPool(pool_id, capacity, sizeof(MetadataBatchBuffer), alignof(MetadataBatchBuffer));
}

std::size_t distributed_diff_flat_slots(std::size_t worker_count, std::size_t async_depth) {
    const std::size_t requested = std::max<std::size_t>(64U, worker_count * async_depth);
    return std::min<std::size_t>(requested, kDistributedDiffFlatBufferSlotCap);
}

std::size_t distributed_diff_control_slots(std::size_t worker_count) {
    const std::size_t requested = std::max<std::size_t>(64U, worker_count * 4U);
    return std::min<std::size_t>(requested, kDistributedDiffControlBufferSlotCap);
}

class SourceBatchRouterJob final : public ThreadedJob {
public:
    struct Stats {
        std::uint64_t source_batches_routed = 0;
        std::uint64_t target_requests_sent = 0;
    };

    SourceBatchRouterJob(BufQueue& input,
                         RawBufferPool& source_pool,
                         RawBufferPool& request_pool,
                         BufQueue& target_requests,
                         std::vector<std::unique_ptr<BufQueue>>& diff_shards)
        : ThreadedJob(1U),
          input_(input),
          source_pool_(source_pool),
          request_pool_(request_pool),
          target_requests_(target_requests),
          diff_shards_(diff_shards) {}

    [[nodiscard]] Stats stats() const {
        return {source_batches_routed_.load(std::memory_order_acquire),
                target_requests_sent_.load(std::memory_order_acquire)};
    }

protected:
    void run_worker(std::size_t worker_index) override {
        BufferHandle handle;
        while (!stop_requested() && wait_for_input(worker_index, input_, handle)) {
            const MetadataBatchBuffer& source_buffer = metadata_batch_buffer(source_pool_, handle);
            const FlatFolderBufferInfo info = flat_folder_buffer_info(source_buffer);
            if (info.sequence == 0U && !info.failed) {
                const std::optional<BufferHandle> acquired = wait_for_pool(worker_index, request_pool_);
                if (!acquired.has_value()) {
                    source_pool_.release(handle);
                    break;
                }
                BufferHandle request = *acquired;
                FileSpec folder;
                folder.rel_path.assign(info.folder_path);
                folder.mtime = info.folder_mtime;
                folder.mode = info.folder_mode;
                folder.uid = info.folder_uid;
                folder.gid = info.folder_gid;
                reset_flat_folder_buffer(metadata_batch_buffer(request_pool_, request),
                                         folder,
                                         0,
                                         true,
                                         false,
                                         {},
                                         0,
                                         0,
                                         0,
                                         0,
                                         0,
                                         0);
                if (!wait_for_output(worker_index, target_requests_, request)) {
                    request_pool_.release(request);
                    source_pool_.release(handle);
                    break;
                }
                target_requests_sent_.fetch_add(1U, std::memory_order_relaxed);
            }
            const std::size_t shard = shard_for_folder_hash(info.folder_hash, diff_shards_.size());
            if (!wait_for_output(worker_index, *diff_shards_[shard], handle)) {
                source_pool_.release(handle);
                break;
            }
            source_batches_routed_.fetch_add(1U, std::memory_order_relaxed);
        }
    }

    void on_stop_requested() override {
        input_.close();
        target_requests_.close();
    }

    void on_all_workers_finished() override {
        target_requests_.close();
    }

private:
    BufQueue& input_;
    RawBufferPool& source_pool_;
    RawBufferPool& request_pool_;
    BufQueue& target_requests_;
    std::vector<std::unique_ptr<BufQueue>>& diff_shards_;
    std::atomic<std::uint64_t> source_batches_routed_ {0};
    std::atomic<std::uint64_t> target_requests_sent_ {0};
};

class TargetFolderScannerBufferJob final : public ThreadedJob {
public:
    struct Stats {
        std::uint64_t folders_scanned = 0;
        std::uint64_t files_seen = 0;
        std::uint64_t logical_size_bytes = 0;
    };

    TargetFolderScannerBufferJob(std::string target_root,
                                 std::string compare_mode,
                                 std::size_t worker_count,
                                 std::size_t async_depth,
                                 BufQueue& requests,
                                 RawBufferPool& request_pool,
                                 RawBufferPool& output_pool,
                                 std::vector<std::unique_ptr<BufQueue>>& diff_shards)
        : ThreadedJob(worker_count),
          target_root_(std::move(target_root)),
          compare_mode_(std::move(compare_mode)),
          async_depth_(std::max<std::size_t>(1U, async_depth)),
          requests_(requests),
          request_pool_(request_pool),
          output_pool_(output_pool),
          diff_shards_(diff_shards) {}

    [[nodiscard]] Stats stats() const {
        return {folders_scanned_.load(std::memory_order_acquire),
                files_seen_.load(std::memory_order_acquire),
                logical_size_bytes_.load(std::memory_order_acquire)};
    }

protected:
    void run_worker(std::size_t worker_index) override {
        auto backend = make_nfs_backend(target_root_);
        auto io_scope = runtime_state_scope(worker_index, RuntimeState::wait_io);
        backend->scan_flat_folders(
            async_depth_,
            [this, worker_index](bool wait_for_work) -> std::optional<FileSpec> {
                if (!wait_until_worker_active(worker_index)) {
                    return std::nullopt;
                }
                BufferHandle request;
                const bool got_request =
                    wait_for_work ? wait_for_input(worker_index, requests_, request) : requests_.try_pop(request);
                if (!got_request) {
                    return std::nullopt;
                }

                const FlatFolderBufferInfo request_info =
                    flat_folder_buffer_info(metadata_batch_buffer(request_pool_, request));
                FileSpec folder;
                folder.rel_path.assign(request_info.folder_path);
                folder.mtime = request_info.folder_mtime;
                folder.mode = request_info.folder_mode;
                folder.uid = request_info.folder_uid;
                folder.gid = request_info.folder_gid;
                request_pool_.release(request);
                return folder;
            },
            [this] {
                return stop_requested();
            },
            [this, worker_index](FlatFolderScanBatch batch) {
                files_seen_.fetch_add(batch.files.size(), std::memory_order_relaxed);
                logical_size_bytes_.fetch_add(flat_folder_logical_size(batch.files), std::memory_order_relaxed);
                const std::uint64_t folder_hash = folder_hash_for_path(batch.folder.rel_path);
                const std::size_t shard = shard_for_folder_hash(folder_hash, diff_shards_.size());
                pack_flat_folder_batch_to_queue_with(
                    batch,
                    compare_mode_,
                    output_pool_,
                    [this, worker_index] {
                        if (auto handle = wait_for_pool(worker_index, output_pool_); handle.has_value()) {
                            return *handle;
                        }
                        throw std::runtime_error("target folder scanner stopped while waiting for output buffer");
                    },
                    [this, worker_index, shard](const BufferHandle& handle) {
                        if (!wait_for_output(worker_index, *diff_shards_[shard], handle)) {
                            output_pool_.release(handle);
                            throw std::runtime_error("target folder scanner output queue closed");
                        }
                    });
                folders_scanned_.fetch_add(1U, std::memory_order_relaxed);
            });
    }

    void on_stop_requested() override {
        requests_.close();
        for (auto& queue : diff_shards_) {
            queue->close();
        }
    }

    void on_all_workers_finished() override {
        for (auto& queue : diff_shards_) {
            queue->close();
        }
    }

private:
    std::string target_root_;
    std::string compare_mode_;
    std::size_t async_depth_ = 1;
    BufQueue& requests_;
    RawBufferPool& request_pool_;
    RawBufferPool& output_pool_;
    std::vector<std::unique_ptr<BufQueue>>& diff_shards_;
    std::atomic<std::uint64_t> folders_scanned_ {0};
    std::atomic<std::uint64_t> files_seen_ {0};
    std::atomic<std::uint64_t> logical_size_bytes_ {0};
};

struct ComparableFlatFile {
    std::uint64_t name_hash = 0;
    std::uint64_t metadata_hash = 0;
    std::uint64_t logical_size = 0;
};

struct PendingDiffFolder {
    std::vector<BufferHandle> source;
    std::vector<BufferHandle> target;
    bool source_done = false;
    bool target_done = false;
};

class FolderDiffShardJob final : public ThreadedJob {
public:
    struct Stats {
        std::uint64_t folders_compared = 0;
        std::uint64_t files_compared = 0;
        std::uint64_t bytes_planned = 0;
        std::uint64_t source_batches = 0;
        std::uint64_t target_batches = 0;
    };

    FolderDiffShardJob(BufQueue& input,
                       RawBufferPool& source_pool,
                       RawBufferPool& target_pool,
                       RawBufferPool& result_pool,
                       BufQueue& results)
        : ThreadedJob(1U),
          input_(input),
          source_pool_(source_pool),
          target_pool_(target_pool),
          result_pool_(result_pool),
          results_(results) {}

    [[nodiscard]] Stats stats() const {
        return {folders_compared_.load(std::memory_order_acquire),
                files_compared_.load(std::memory_order_acquire),
                bytes_planned_.load(std::memory_order_acquire),
                source_batches_.load(std::memory_order_acquire),
                target_batches_.load(std::memory_order_acquire)};
    }

protected:
    void run_worker(std::size_t worker_index) override {
        BufferHandle handle;
        while (!stop_requested() && wait_for_input(worker_index, input_, handle)) {
            if (handle.pool_id == source_pool_.pool_id()) {
                source_batches_.fetch_add(1U, std::memory_order_relaxed);
                accept_batch(handle, true, worker_index);
            } else if (handle.pool_id == target_pool_.pool_id()) {
                target_batches_.fetch_add(1U, std::memory_order_relaxed);
                accept_batch(handle, false, worker_index);
            } else {
                throw std::runtime_error("diff shard received a buffer from an unexpected pool");
            }
        }
        flush_missing_targets(worker_index);
        flush_result_buffer(worker_index);
    }

    void on_stop_requested() override {
        input_.close();
    }

private:
    void accept_batch(const BufferHandle& handle, bool source_side, std::size_t worker_index) {
        RawBufferPool& pool = source_side ? source_pool_ : target_pool_;
        const FlatFolderBufferInfo info = flat_folder_buffer_info(metadata_batch_buffer(pool, handle));
        PendingDiffFolder& pending = pending_[info.folder_hash];
        if (source_side) {
            pending.source.push_back(handle);
            pending.source_done = pending.source_done || info.final_batch;
        } else {
            pending.target.push_back(handle);
            pending.target_done = pending.target_done || info.final_batch;
        }
        if (pending.source_done && pending.target_done) {
            compare_ready_folder(info.folder_hash, pending, worker_index);
            pending_.erase(info.folder_hash);
        }
    }

    void flush_missing_targets(std::size_t worker_index) {
        std::vector<std::uint64_t> ready;
        ready.reserve(pending_.size());
        for (const auto& [folder_hash, pending] : pending_) {
            if (pending.source_done) {
                ready.push_back(folder_hash);
            }
        }
        for (std::uint64_t folder_hash : ready) {
            auto it = pending_.find(folder_hash);
            if (it == pending_.end()) {
                continue;
            }
            compare_ready_folder(folder_hash, it->second, worker_index);
            pending_.erase(it);
        }
    }

    static void collect_files(const std::vector<BufferHandle>& handles,
                              RawBufferPool& pool,
                              std::vector<ComparableFlatFile>& out) {
        for (const BufferHandle& handle : handles) {
            visit_flat_folder_children(metadata_batch_buffer(pool, handle), [&out](FlatFolderChildView child) {
                if (!child.is_file) {
                    return;
                }
                out.push_back(ComparableFlatFile{child.name_hash, child.metadata_hash, child.logical_size});
            });
        }
        std::sort(out.begin(), out.end(), [](const ComparableFlatFile& lhs, const ComparableFlatFile& rhs) {
            return lhs.name_hash < rhs.name_hash;
        });
    }

    void compare_ready_folder(std::uint64_t folder_hash,
                              PendingDiffFolder& pending,
                              std::size_t worker_index) {
        (void)folder_hash;
        FolderDiffSummary summary;
        const bool has_source = !pending.source.empty();
        const bool has_target = !pending.target.empty();
        FlatFolderBufferInfo source_info;
        FlatFolderBufferInfo target_info;
        if (has_source) {
            source_info = flat_folder_buffer_info(metadata_batch_buffer(source_pool_, pending.source.front()));
            summary.rel_path = source_info.folder_path;
            summary.source_scan_started_unix_ns = source_info.scan_started_unix_ns;
            summary.source_scan_finished_unix_ns = source_info.scan_finished_unix_ns;
            summary.source_file_count = source_info.total_file_count;
            summary.source_folder_count = source_info.total_folder_count;
            summary.source_logical_size_bytes = source_info.total_logical_size_bytes;
        }
        if (has_target) {
            target_info = flat_folder_buffer_info(metadata_batch_buffer(target_pool_, pending.target.front()));
            if (!has_source) {
                summary.rel_path = target_info.folder_path;
            }
            summary.target_scan_started_unix_ns = target_info.scan_started_unix_ns;
            summary.target_scan_finished_unix_ns = target_info.scan_finished_unix_ns;
            summary.target_file_count = target_info.total_file_count;
            summary.target_folder_count = target_info.total_folder_count;
            summary.target_logical_size_bytes = target_info.total_logical_size_bytes;
        }

        if (!has_source || source_info.failed) {
            summary.status = 1U;
            summary.files_failed = 1U;
            summary.error = has_source ? source_info.error : std::string_view("missing source folder batch");
        } else if (!has_target || target_info.failed) {
            summary.status = 2U;
            summary.files_new = source_info.total_file_count;
            summary.new_logical_size_bytes = source_info.total_logical_size_bytes;
            summary.bytes_planned = source_info.total_logical_size_bytes;
            summary.error = has_target ? target_info.error : std::string_view("missing target folder batch");
        } else if (source_info.total_file_count == target_info.total_file_count &&
                   source_info.total_folder_count == target_info.total_folder_count &&
                   source_info.total_logical_size_bytes == target_info.total_logical_size_bytes &&
                   source_info.metadata_hash == target_info.metadata_hash) {
            summary.files_same = source_info.total_file_count;
            summary.same_logical_size_bytes = source_info.total_logical_size_bytes;
        } else {
            std::vector<ComparableFlatFile> source_files;
            std::vector<ComparableFlatFile> target_files;
            source_files.reserve(static_cast<std::size_t>(std::min<std::uint64_t>(
                source_info.total_file_count, static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max()))));
            target_files.reserve(static_cast<std::size_t>(std::min<std::uint64_t>(
                target_info.total_file_count, static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max()))));
            collect_files(pending.source, source_pool_, source_files);
            collect_files(pending.target, target_pool_, target_files);
            std::size_t source_index = 0;
            std::size_t target_index = 0;
            while (source_index < source_files.size() || target_index < target_files.size()) {
                if (target_index >= target_files.size() ||
                    (source_index < source_files.size() &&
                     source_files[source_index].name_hash < target_files[target_index].name_hash)) {
                    ++summary.files_new;
                    summary.new_logical_size_bytes += source_files[source_index].logical_size;
                    summary.bytes_planned += source_files[source_index].logical_size;
                    ++source_index;
                    continue;
                }
                if (source_index >= source_files.size() ||
                    target_files[target_index].name_hash < source_files[source_index].name_hash) {
                    ++summary.files_target_only;
                    summary.target_only_logical_size_bytes += target_files[target_index].logical_size;
                    ++target_index;
                    continue;
                }
                if (source_files[source_index].metadata_hash == target_files[target_index].metadata_hash) {
                    ++summary.files_same;
                    summary.same_logical_size_bytes += source_files[source_index].logical_size;
                } else {
                    ++summary.files_changed;
                    summary.changed_logical_size_bytes += source_files[source_index].logical_size;
                    summary.bytes_planned += source_files[source_index].logical_size;
                }
                ++source_index;
                ++target_index;
            }
        }

        summary.result_sent_unix_ns = now_unix_ns();
        folders_compared_.fetch_add(1U, std::memory_order_relaxed);
        files_compared_.fetch_add(summary.source_file_count + summary.files_target_only,
                                  std::memory_order_relaxed);
        bytes_planned_.fetch_add(summary.bytes_planned, std::memory_order_relaxed);
        emit_summary(summary, worker_index);
        for (const BufferHandle& handle : pending.source) {
            source_pool_.release(handle);
        }
        for (const BufferHandle& handle : pending.target) {
            target_pool_.release(handle);
        }
    }

    void emit_summary(const FolderDiffSummary& summary, std::size_t worker_index) {
        if (!open_result_.has_value()) {
            open_result_ = acquire_result_buffer(worker_index);
            reset_diff_result_buffer(metadata_batch_buffer(result_pool_, *open_result_));
        }
        if (!append_diff_result(metadata_batch_buffer(result_pool_, *open_result_), summary)) {
            flush_result_buffer(worker_index);
            open_result_ = acquire_result_buffer(worker_index);
            reset_diff_result_buffer(metadata_batch_buffer(result_pool_, *open_result_));
            if (!append_diff_result(metadata_batch_buffer(result_pool_, *open_result_), summary)) {
                result_pool_.release(*open_result_);
                open_result_.reset();
                throw std::runtime_error("single diff summary is too large for result buffer");
            }
        }
        const auto now = std::chrono::steady_clock::now();
        if (last_result_flush_.time_since_epoch().count() == 0) {
            last_result_flush_ = now;
        }
        if (metadata_batch_buffer(result_pool_, *open_result_).record_count >= kDistributedDiffResultFlushRecords ||
            now - last_result_flush_ >= kDistributedDiffResultFlushInterval) {
            flush_result_buffer(worker_index);
        }
    }

    [[nodiscard]] BufferHandle acquire_result_buffer(std::size_t worker_index) {
        if (auto handle = wait_for_pool(worker_index, result_pool_); handle.has_value()) {
            return *handle;
        }
        throw std::runtime_error("folder diff shard stopped while waiting for result buffer");
    }

    void flush_result_buffer(std::size_t worker_index) {
        if (!open_result_.has_value()) {
            return;
        }
        if (metadata_batch_buffer(result_pool_, *open_result_).record_count == 0U) {
            result_pool_.release(*open_result_);
            open_result_.reset();
            return;
        }
        if (!wait_for_output(worker_index, results_, *open_result_)) {
            result_pool_.release(*open_result_);
            open_result_.reset();
            throw std::runtime_error("folder diff result queue closed");
        }
        open_result_.reset();
        last_result_flush_ = std::chrono::steady_clock::now();
    }

    BufQueue& input_;
    RawBufferPool& source_pool_;
    RawBufferPool& target_pool_;
    RawBufferPool& result_pool_;
    BufQueue& results_;
    std::unordered_map<std::uint64_t, PendingDiffFolder> pending_;
    std::optional<BufferHandle> open_result_;
    std::chrono::steady_clock::time_point last_result_flush_ {};
    std::atomic<std::uint64_t> folders_compared_ {0};
    std::atomic<std::uint64_t> files_compared_ {0};
    std::atomic<std::uint64_t> bytes_planned_ {0};
    std::atomic<std::uint64_t> source_batches_ {0};
    std::atomic<std::uint64_t> target_batches_ {0};
};

class DiffResultReportWriterJob final : public ThreadedJob {
public:
    DiffResultReportWriterJob(BufQueue& input,
                              RawBufferPool& input_pool,
                              std::filesystem::path output_path)
        : ThreadedJob(1U),
          input_(input),
          input_pool_(input_pool),
          output_path_(std::move(output_path)) {}

    [[nodiscard]] DistributedDiffRunReport report() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return report_;
    }

protected:
    void on_starting() override {
        if (!output_path_.empty()) {
            if (output_path_.has_parent_path()) {
                std::filesystem::create_directories(output_path_.parent_path());
            }
            output_.open(output_path_);
            if (!output_) {
                throw std::runtime_error("failed to open distributed diff folder report: " +
                                         output_path_.string());
            }
            write_folder_diff_csv_header(output_);
        }
    }

    void run_worker(std::size_t worker_index) override {
        BufferHandle handle;
        while (!stop_requested() && wait_for_input(worker_index, input_, handle)) {
            MetadataBatchBuffer& buffer = metadata_batch_buffer(input_pool_, handle);
            visit_diff_results(buffer, [this, worker_index](FolderDiffSummary summary) {
                summary.result_received_unix_ns = now_unix_ns();
                std::lock_guard<std::mutex> lock(mutex_);
                merge_folder_diff_summary(report_, summary);
                if (output_) {
                    auto io_scope = runtime_state_scope(worker_index, RuntimeState::wait_io);
                    write_folder_diff_csv_row(output_, summary);
                }
            });
            input_pool_.release(handle);
        }
    }

    void on_stop_requested() override {
        input_.close();
    }

private:
    BufQueue& input_;
    RawBufferPool& input_pool_;
    std::filesystem::path output_path_;
    mutable std::mutex mutex_;
    DistributedDiffRunReport report_;
    std::ofstream output_;
};

void append_fixed_decimal(std::string& out, std::uint64_t value, std::size_t width) {
    const std::size_t old_size = out.size();
    out.resize(old_size + width);
    for (std::size_t offset = 0; offset < width; ++offset) {
        const std::size_t digit_index = old_size + width - 1U - offset;
        out[digit_index] = static_cast<char>('0' + (value % 10U));
        value /= 10U;
    }
}

std::string synthetic_diff_folder_path(std::uint64_t folder_index) {
    std::string path;
    path.reserve(22U);
    path.append("synthetic/dir_");
    append_fixed_decimal(path, folder_index, 8U);
    return path;
}

std::string synthetic_diff_file_path(std::uint64_t folder_index, std::uint64_t file_index) {
    std::string path;
    path.reserve(40U);
    path.append("synthetic/dir_");
    append_fixed_decimal(path, folder_index, 8U);
    path.append("/file_");
    append_fixed_decimal(path, file_index, 12U);
    path.append(".dat");
    return path;
}

std::uint64_t synthetic_diff_files_in_folder(std::uint64_t file_count,
                                             std::uint64_t folder_count,
                                             std::uint64_t folder_index) {
    if (folder_count == 0U || folder_index >= folder_count) {
        return 0;
    }
    const std::uint64_t base = file_count / folder_count;
    const std::uint64_t remainder = file_count % folder_count;
    return base + (folder_index < remainder ? 1U : 0U);
}

std::uint64_t synthetic_diff_file_size(std::uint64_t file_index, std::uint64_t average_file_size) {
    if (average_file_size == 0U) {
        return 0;
    }
    const std::uint64_t mixed = (file_index * 11400714819323198485ULL) ^ (file_index >> 17U);
    return 1U + (mixed % (average_file_size * 2U));
}

FlatFolderScanBatch make_synthetic_diff_batch(std::uint64_t file_count,
                                              std::uint64_t folder_count,
                                              std::uint64_t average_file_size,
                                              std::uint64_t folder_index) {
    FlatFolderScanBatch batch;
    batch.folder.rel_path = synthetic_diff_folder_path(folder_index);
    batch.folder.mtime = 1'700'000'000'000'000'000ULL + folder_index;
    batch.folder.mode = 0755;
    batch.folder.uid = static_cast<std::uint32_t>(1000U + (folder_index % 97U));
    batch.folder.gid = static_cast<std::uint32_t>(1000U + (folder_index % 89U));

    const std::uint64_t files_in_folder =
        synthetic_diff_files_in_folder(file_count, folder_count, folder_index);
    batch.files.reserve(static_cast<std::size_t>(files_in_folder));
    for (std::uint64_t offset = 0; offset < files_in_folder; ++offset) {
        const std::uint64_t file_index = folder_index + (offset * folder_count);
        FileSpec file;
        file.rel_path = synthetic_diff_file_path(folder_index, file_index);
        file.declared_size = synthetic_diff_file_size(file_index, average_file_size);
        file.mtime = 1'700'000'000'000'000'000ULL + file_index;
        file.mode = 0644;
        file.uid = static_cast<std::uint32_t>(1000U + (file_index % 97U));
        file.gid = static_cast<std::uint32_t>(1000U + (file_index % 89U));
        batch.files.push_back(std::move(file));
    }
    return batch;
}

void synthetic_diff_source_worker(std::uint64_t file_count,
                                  std::uint64_t folder_count,
                                  std::uint64_t average_file_size,
                                  std::atomic<std::uint64_t>& next_folder,
                                  DiffTargetFolderQueue& target_queue,
                                  DiffBatchQueueShards& source_batches,
                                  std::atomic<std::uint64_t>& files_generated,
                                  std::atomic<std::uint64_t>& folders_generated,
                                  std::atomic<std::uint64_t>& bytes_generated,
                                  DiffPipelineTimingCounters* timing = nullptr) {
    try {
        for (;;) {
            const std::uint64_t folder_index = next_folder.fetch_add(1U, std::memory_order_relaxed);
            if (folder_index >= folder_count) {
                break;
            }
            FlatFolderScanBatch batch =
                make_synthetic_diff_batch(file_count, folder_count, average_file_size, folder_index);
            std::uint64_t logical_size = 0;
            for (const auto& file : batch.files) {
                logical_size += file_spec_logical_size(file);
            }
            FileSpec target_folder = batch.folder;
            target_folder.need_check = true;
            const auto target_wait_started_at = std::chrono::steady_clock::now();
            if (!enqueue_diff_target_folder(target_queue, std::move(target_folder))) {
                break;
            }
            if (timing != nullptr) {
                add_elapsed_ns(timing->source_wait_target_queue_ns,
                               target_wait_started_at,
                               std::chrono::steady_clock::now());
            }
            const std::uint64_t file_total = static_cast<std::uint64_t>(batch.files.size());
            const auto batch_wait_started_at = std::chrono::steady_clock::now();
            if (!push_diff_batch(source_batches, std::move(batch))) {
                break;
            }
            if (timing != nullptr) {
                add_elapsed_ns(timing->source_wait_batch_queue_ns,
                               batch_wait_started_at,
                               std::chrono::steady_clock::now());
            }
            files_generated.fetch_add(file_total, std::memory_order_relaxed);
            folders_generated.fetch_add(1U, std::memory_order_relaxed);
            bytes_generated.fetch_add(logical_size, std::memory_order_relaxed);
        }
    } catch (...) {
        fail_diff_target_folder_work(target_queue);
        fail_diff_batch_queue(source_batches);
    }
    finish_diff_batch_producer(source_batches);
}

void fake_remote_request_receiver_worker(DiffTargetFolderQueue& target_queue,
                                         FakeRemoteProcessorQueue& processor_queue,
                                         DiffBatchQueueShards& target_batches,
                                         DiffPipelineTimingCounters* timing = nullptr) {
    try {
        for (;;) {
            const auto request_wait_started_at = std::chrono::steady_clock::now();
            std::optional<FileSpec> folder = take_diff_target_folder_work(target_queue, true);
            if (timing != nullptr) {
                add_elapsed_ns(timing->fake_remote_wait_request_ns,
                               request_wait_started_at,
                               std::chrono::steady_clock::now());
            }
            if (!folder.has_value()) {
                break;
            }
            const auto processor_queue_wait_started_at = std::chrono::steady_clock::now();
            const bool accepted = push_fake_remote_request(processor_queue, std::move(*folder));
            if (timing != nullptr) {
                add_elapsed_ns(timing->fake_remote_wait_processor_queue_ns,
                               processor_queue_wait_started_at,
                               std::chrono::steady_clock::now());
            }
            finish_diff_target_folder_work(target_queue);
            if (!accepted) {
                break;
            }
        }
    } catch (...) {
        fail_diff_target_folder_work(target_queue);
        fail_fake_remote_processor_queue(processor_queue);
        fail_diff_batch_queue(target_batches);
    }
    finish_fake_remote_request_producer(processor_queue);
}

void fake_remote_processor_worker(std::uint64_t file_count,
                                  std::uint64_t folder_count,
                                  std::uint64_t average_file_size,
                                  std::uint64_t remote_delay_microseconds,
                                  FakeRemoteProcessorQueue& processor_queue,
                                  DiffBatchQueueShards& target_batches,
                                  std::atomic<std::uint64_t>& folders_checked,
                                  DiffPipelineTimingCounters* timing = nullptr) {
    try {
        for (;;) {
            std::optional<FileSpec> folder = take_fake_remote_request(processor_queue);
            if (!folder.has_value()) {
                break;
            }
            if (remote_delay_microseconds != 0U) {
                const auto delay_started_at = std::chrono::steady_clock::now();
                std::this_thread::sleep_for(std::chrono::microseconds(remote_delay_microseconds));
                if (timing != nullptr) {
                    add_elapsed_ns(timing->fake_remote_delay_ns,
                                   delay_started_at,
                                   std::chrono::steady_clock::now());
                }
            }
            std::uint64_t folder_index = 0;
            const std::string& rel_path = folder->rel_path;
            if (rel_path.size() >= 8U) {
                const std::string_view suffix(rel_path.data() + rel_path.size() - 8U, 8U);
                std::from_chars(suffix.data(), suffix.data() + suffix.size(), folder_index);
            }
            FlatFolderScanBatch batch =
                make_synthetic_diff_batch(file_count, folder_count, average_file_size, folder_index);
            const auto batch_wait_started_at = std::chrono::steady_clock::now();
            if (!push_diff_batch(target_batches, std::move(batch))) {
                break;
            }
            if (timing != nullptr) {
                add_elapsed_ns(timing->fake_remote_wait_batch_queue_ns,
                               batch_wait_started_at,
                               std::chrono::steady_clock::now());
            }
            folders_checked.fetch_add(1U, std::memory_order_relaxed);
        }
    } catch (...) {
        fail_fake_remote_processor_queue(processor_queue);
        fail_diff_batch_queue(target_batches);
    }
    finish_diff_batch_producer(target_batches);
}

std::size_t queued_data_read_files(DataReadFileQueue& queue) {
    std::lock_guard<std::mutex> lock(queue.mutex);
    return queue.files.size();
}

bool data_file_input_done_and_empty(DataReadFileQueue& queue) {
    std::lock_guard<std::mutex> lock(queue.mutex);
    return queue.input_done && queue.files.empty();
}

std::size_t queued_folder_ready_batches(FolderReadyBatchQueue& queue) {
    std::lock_guard<std::mutex> lock(queue.mutex);
    return queue.batches.size();
}

bool folder_ready_timer_expired(const FolderReadyBatchQueue& queue) {
    return queue.stop_at.has_value() && std::chrono::steady_clock::now() >= *queue.stop_at;
}

void fail_folder_ready_work(FolderReadyBatchQueue& queue, std::exception_ptr error = std::current_exception()) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.stop = true;
        if (!queue.error && error != nullptr) {
            queue.error = error;
        }
    }
    queue.cv_not_empty.notify_all();
    queue.cv_not_full.notify_all();
}

void request_folder_ready_stop(FolderReadyBatchQueue& queue) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.stop = true;
        queue.batches.clear();
    }
    queue.cv_not_empty.notify_all();
    queue.cv_not_full.notify_all();
}

bool enqueue_folder_ready_batch(FolderReadyBatchQueue& queue, FolderReadyFileBatch&& batch) {
    std::unique_lock<std::mutex> lock(queue.mutex);
    const auto ready = [&queue]() {
        return queue.stop || queue.error || folder_ready_timer_expired(queue) ||
               queue.batches.size() < queue.max_entries;
    };
    if (queue.stop_at.has_value()) {
        queue.cv_not_full.wait_until(lock, *queue.stop_at, ready);
    } else {
        queue.cv_not_full.wait(lock, ready);
    }

    if (folder_ready_timer_expired(queue)) {
        queue.stop = true;
        queue.batches.clear();
    }
    if (queue.stop || queue.error) {
        queue.cv_not_empty.notify_all();
        queue.cv_not_full.notify_all();
        return false;
    }

    queue.batches.push_back(std::move(batch));
    queue.high_watermark = std::max(queue.high_watermark, queue.batches.size());
    lock.unlock();
    queue.cv_not_empty.notify_one();
    return true;
}

std::optional<FolderReadyFileBatch> take_folder_ready_batch(FolderReadyBatchQueue& queue) {
    std::unique_lock<std::mutex> lock(queue.mutex);
    const auto ready = [&queue]() {
        return queue.stop || queue.error || !queue.batches.empty() || queue.input_done ||
               folder_ready_timer_expired(queue);
    };
    if (queue.stop_at.has_value()) {
        queue.cv_not_empty.wait_until(lock, *queue.stop_at, ready);
    } else {
        queue.cv_not_empty.wait(lock, ready);
    }

    if (folder_ready_timer_expired(queue)) {
        queue.stop = true;
        queue.batches.clear();
    }
    if (queue.stop || queue.error || queue.batches.empty()) {
        return std::nullopt;
    }

    FolderReadyFileBatch batch = std::move(queue.batches.front());
    queue.batches.pop_front();
    lock.unlock();
    queue.cv_not_full.notify_one();
    return batch;
}

void mark_folder_ready_input_done(FolderReadyBatchQueue& queue) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.input_done = true;
    }
    queue.cv_not_empty.notify_all();
    queue.cv_not_full.notify_all();
}

bool wait_until_scanner_worker_active(std::size_t worker_index,
                                      ScannerCapacityControl* control,
                                      const std::function<bool()>& should_stop) {
    if (control == nullptr) {
        return !should_stop();
    }
    if (worker_index < control->active_workers.load(std::memory_order_acquire)) {
        return !should_stop();
    }
    std::unique_lock<std::mutex> lock(control->mutex);
    while (!should_stop() &&
           worker_index >= control->active_workers.load(std::memory_order_acquire)) {
        control->cv.wait_for(lock, std::chrono::milliseconds(100));
    }
    return !should_stop();
}

std::size_t set_scanner_active_workers(ScannerCapacityControl& control,
                                       std::size_t requested,
                                       std::size_t max_workers) {
    const std::size_t clamped = std::clamp<std::size_t>(requested, 1U, std::max<std::size_t>(1U, max_workers));
    control.active_workers.store(clamped, std::memory_order_release);
    control.cv.notify_all();
    return clamped;
}

SplitScannerCapacityDecision choose_split_scanner_capacity_impl(
    std::size_t base_small_scanners,
    std::size_t base_large_scanners,
    std::size_t large_scanner_floor,
    std::uint32_t large_reader_small_priority_percent) noexcept {
    base_small_scanners = std::max<std::size_t>(1U, base_small_scanners);
    base_large_scanners = std::max<std::size_t>(1U, base_large_scanners);
    large_scanner_floor = std::clamp<std::size_t>(
        std::max<std::size_t>(1U, large_scanner_floor),
        1U,
        base_large_scanners);

    SplitScannerCapacityDecision decision;
    decision.small_scanners = base_small_scanners;
    decision.large_scanners = base_large_scanners;
    if (large_reader_small_priority_percent == 0U) {
        return decision;
    }

    const std::size_t shift = base_large_scanners - large_scanner_floor;
    decision.small_scanners = base_small_scanners + shift;
    decision.large_scanners = large_scanner_floor;
    return decision;
}

void record_data_read_metadata(DataReadBenchmarkStats& stats,
                               std::size_t files_found,
                               std::size_t folders_found,
                               std::uint64_t logical_size_bytes) {
    stats.files_found.fetch_add(files_found, std::memory_order_relaxed);
    stats.folders_found.fetch_add(folders_found, std::memory_order_relaxed);
    stats.logical_size_bytes.fetch_add(logical_size_bytes, std::memory_order_relaxed);
}

void record_data_read_bytes(DataReadBenchmarkStats& stats, std::uint64_t bytes_read) {
    stats.bytes_read.fetch_add(bytes_read, std::memory_order_relaxed);
}

void record_small_data_read_bytes(DataReadBenchmarkStats& stats, std::uint64_t bytes_read) {
    record_data_read_bytes(stats, bytes_read);
    stats.small_bytes_read.fetch_add(bytes_read, std::memory_order_relaxed);
}

void record_large_data_read_bytes(DataReadBenchmarkStats& stats, std::uint64_t bytes_read) {
    record_data_read_bytes(stats, bytes_read);
    stats.large_bytes_read.fetch_add(bytes_read, std::memory_order_relaxed);
}

void record_data_read_file(DataReadBenchmarkStats& stats) {
    stats.files_read.fetch_add(1, std::memory_order_relaxed);
}

void record_small_data_read_file(DataReadBenchmarkStats& stats) {
    record_data_read_file(stats);
    stats.small_files_read.fetch_add(1, std::memory_order_relaxed);
}

void record_large_data_read_file(DataReadBenchmarkStats& stats) {
    record_data_read_file(stats);
    stats.large_files_read.fetch_add(1, std::memory_order_relaxed);
}

void record_current_split_data_read_bytes(DataReadBenchmarkStats& stats, std::uint64_t bytes_read) {
    if (current_split_data_read_route == SplitDataReadRoute::Small) {
        record_small_data_read_bytes(stats, bytes_read);
    } else {
        record_large_data_read_bytes(stats, bytes_read);
    }
}

void record_current_split_data_read_file(DataReadBenchmarkStats& stats) {
    if (current_split_data_read_route == SplitDataReadRoute::Small) {
        record_small_data_read_file(stats);
    } else {
        record_large_data_read_file(stats);
    }
}

void print_data_buffer_read_stats(const DataReadBenchmarkStats& stats,
                                  DataReadFileQueue& file_queue,
                                  const BufQueue& data_queue) {
    const DataReadBenchmarkSnapshot snapshot = snapshot_data_read_stats(stats);
    const NfsAsyncReadLatencySnapshot read_latency = snapshot_nfs_async_read_latency_metrics();
    const NfsAsyncCommandLatencySnapshot command_latency = snapshot_nfs_async_command_latency_metrics();
    const NfsReaddirplusPageSnapshot readdirplus = snapshot_nfs_readdirplus_page_metrics();
    const double avg_latency_ms = read_latency.completed != 0U
                                      ? static_cast<double>(read_latency.latency_ns) /
                                            static_cast<double>(read_latency.completed) / 1'000'000.0
                                      : 0.0;
    const double max_latency_ms = static_cast<double>(read_latency.max_latency_ns) / 1'000'000.0;
    const double avg_completion_bytes = read_latency.completed != 0U
                                            ? static_cast<double>(read_latency.bytes_completed) /
                                                  static_cast<double>(read_latency.completed)
                                            : 0.0;
    const double avg_open_ms = command_latency.open_completed != 0U
                                   ? static_cast<double>(command_latency.open_latency_ns) /
                                         static_cast<double>(command_latency.open_completed) / 1'000'000.0
                                   : 0.0;
    const double avg_close_ms = command_latency.close_completed != 0U
                                    ? static_cast<double>(command_latency.close_latency_ns) /
                                          static_cast<double>(command_latency.close_completed) / 1'000'000.0
                                    : 0.0;
    const double avg_readdirplus_page_ms = readdirplus.pages != 0U
                                               ? static_cast<double>(readdirplus.page_latency_ns) /
                                                     static_cast<double>(readdirplus.pages) / 1'000'000.0
                                               : 0.0;
    const double avg_readdirplus_decode_ms = readdirplus.pages != 0U
                                                 ? static_cast<double>(readdirplus.decode_latency_ns) /
                                                       static_cast<double>(readdirplus.pages) / 1'000'000.0
                                                 : 0.0;
    const double avg_readdirplus_entries = readdirplus.pages != 0U
                                               ? static_cast<double>(readdirplus.entries) /
                                                     static_cast<double>(readdirplus.pages)
                                               : 0.0;
    std::cerr << "data_read_stats bytes_per_second=" << snapshot.bytes_per_second
              << " gigabits_per_second=" << snapshot.gigabits_per_second
              << " files_per_second=" << snapshot.files_per_second
              << " bytes_read=" << snapshot.bytes_read
              << " files_read=" << snapshot.files_read
              << " files_found=" << snapshot.files_found
              << " small_files_read=" << snapshot.small_files_read
              << " large_files_read=" << snapshot.large_files_read
              << " small_files_found=" << snapshot.small_files_found
              << " large_files_found=" << snapshot.large_files_found
              << " small_bytes_read=" << snapshot.small_bytes_read
              << " large_bytes_read=" << snapshot.large_bytes_read
              << " folders_found=" << snapshot.folders_found
              << " logical_size_bytes=" << snapshot.logical_size_bytes
              << " async_reads_queued=" << read_latency.queued
              << " async_reads_completed=" << read_latency.completed
              << " async_read_short=" << read_latency.short_reads
              << " async_read_failed=" << read_latency.failed
              << " async_read_zero=" << read_latency.zero_reads
              << " async_read_avg_latency_ms=" << avg_latency_ms
              << " async_read_max_latency_ms=" << max_latency_ms
              << " async_read_avg_completion_bytes=" << avg_completion_bytes
              << " async_open_completed=" << command_latency.open_completed
              << " async_open_failed=" << command_latency.open_failed
              << " async_open_avg_latency_ms=" << avg_open_ms
              << " async_open_max_latency_ms=" << static_cast<double>(command_latency.open_max_latency_ns) / 1'000'000.0
              << " async_close_completed=" << command_latency.close_completed
              << " async_close_failed=" << command_latency.close_failed
              << " async_close_avg_latency_ms=" << avg_close_ms
              << " async_close_max_latency_ms=" << static_cast<double>(command_latency.close_max_latency_ns) / 1'000'000.0
              << " readdirplus_pages=" << readdirplus.pages
              << " readdirplus_failed_pages=" << readdirplus.failed_pages
              << " readdirplus_entries=" << readdirplus.entries
              << " readdirplus_files=" << readdirplus.files
              << " readdirplus_directories=" << readdirplus.directories
              << " readdirplus_avg_entries_per_page=" << avg_readdirplus_entries
              << " readdirplus_requested_bytes=" << readdirplus.requested_bytes
              << " readdirplus_avg_page_latency_ms=" << avg_readdirplus_page_ms
              << " readdirplus_max_page_latency_ms="
              << static_cast<double>(readdirplus.max_page_latency_ns) / 1'000'000.0
              << " readdirplus_avg_decode_ms=" << avg_readdirplus_decode_ms
              << " readdirplus_max_decode_ms="
              << static_cast<double>(readdirplus.max_decode_latency_ns) / 1'000'000.0
              << " async_read_buckets_lt100us=" << read_latency.latency_buckets[0]
              << " lt500us=" << read_latency.latency_buckets[1]
              << " lt1ms=" << read_latency.latency_buckets[2]
              << " lt5ms=" << read_latency.latency_buckets[3]
              << " lt10ms=" << read_latency.latency_buckets[4]
              << " lt50ms=" << read_latency.latency_buckets[5]
              << " lt100ms=" << read_latency.latency_buckets[6]
              << " ge100ms=" << read_latency.latency_buckets[7]
              << " queued_files=" << queued_data_read_files(file_queue)
              << " data_queue_depth=" << data_queue.size()
              << " elapsed_seconds=" << snapshot.elapsed_seconds << '\n';
}

void run_data_buffer_read_stats_printer(DataReadBenchmarkStats& stats,
                                        DataReadFileQueue& file_queue,
                                        const BufQueue& data_queue) {
    const auto interval = std::chrono::seconds(stats.print_interval_seconds);
    std::unique_lock<std::mutex> lock(stats.printer_mutex);
    while (true) {
        if (stats.printer_cv.wait_for(lock, interval, [&stats] {
                return stats.printer_done.load(std::memory_order_relaxed);
            })) {
            break;
        }
        std::lock_guard<std::mutex> print_lock(stats.print_mutex);
        stats.last_print_at = std::chrono::steady_clock::now();
        print_data_buffer_read_stats(stats, file_queue, data_queue);
    }
}

void print_data_buffer_write_stats(const DataReadBenchmarkStats& stats,
                                   DataReadFileQueue& file_queue,
                                   const ShardedBufQueue& data_queue,
                                   const TargetDataWriterJob& writer) {
    const DataReadBenchmarkSnapshot snapshot = snapshot_data_read_stats(stats);
    const TargetWriterStats writer_stats = writer.stats();
    const double write_gbps = snapshot.elapsed_seconds > 0.0
                                  ? static_cast<double>(writer_stats.bytes_written) * 8.0 /
                                        snapshot.elapsed_seconds / 1'000'000'000.0
                                  : 0.0;
    const double write_files_per_second = snapshot.elapsed_seconds > 0.0
                                              ? static_cast<double>(writer_stats.files_written) /
                                                    snapshot.elapsed_seconds
                                              : 0.0;
    std::cerr << "data_write_stats read_gigabits_per_second=" << snapshot.gigabits_per_second
              << " read_files_per_second=" << snapshot.files_per_second
              << " write_gigabits_per_second=" << write_gbps
              << " write_files_per_second=" << write_files_per_second
              << " files_found=" << snapshot.files_found
              << " files_read=" << snapshot.files_read
              << " files_written=" << writer_stats.files_written
              << " files_failed=" << snapshot.files_failed
              << " write_failed=" << writer_stats.files_failed
              << " bytes_read=" << snapshot.bytes_read
              << " bytes_written=" << writer_stats.bytes_written
              << " queued_files=" << queued_data_read_files(file_queue)
              << " data_queue_depth=" << data_queue.size()
              << " data_queue_high_watermark=" << data_queue.high_watermark()
              << " data_queue_shards=" << data_queue.shard_count()
              << " elapsed_seconds=" << snapshot.elapsed_seconds << '\n';
}

void run_data_buffer_write_stats_printer(DataReadBenchmarkStats& stats,
                                         DataReadFileQueue& file_queue,
                                         const ShardedBufQueue& data_queue,
                                         const TargetDataWriterJob& writer) {
    const auto interval = std::chrono::seconds(stats.print_interval_seconds);
    std::unique_lock<std::mutex> lock(stats.printer_mutex);
    while (true) {
        if (stats.printer_cv.wait_for(lock, interval, [&stats] {
                return stats.printer_done.load(std::memory_order_relaxed);
            })) {
            break;
        }
        std::lock_guard<std::mutex> print_lock(stats.print_mutex);
        stats.last_print_at = std::chrono::steady_clock::now();
        print_data_buffer_write_stats(stats, file_queue, data_queue, writer);
    }
}

struct DirectTargetWriterStats {
    std::atomic<std::uint64_t> buffers_processed {0};
    std::atomic<std::uint64_t> files_written {0};
    std::atomic<std::uint64_t> files_failed {0};
    std::atomic<std::uint64_t> bytes_written {0};

    [[nodiscard]] TargetWriterStats snapshot() const {
        TargetWriterStats out;
        out.worker_count = 0;
        out.buffers_processed = buffers_processed.load(std::memory_order_acquire);
        out.files_written = files_written.load(std::memory_order_acquire);
        out.files_failed = files_failed.load(std::memory_order_acquire);
        out.bytes_written = bytes_written.load(std::memory_order_acquire);
        return out;
    }
};

struct alignas(64) CopyTargetEngineTelemetry {
    std::atomic<std::int64_t> rx_queue_depth {0};
    std::atomic<std::int64_t> mkdir_queue_depth {0};
    std::atomic<std::int64_t> file_create_queue_depth {0};
    std::atomic<std::int64_t> small_write_queue_depth {0};
    std::atomic<std::int64_t> medium_write_queue_depth {0};
    std::atomic<std::int64_t> large_write_queue_depth {0};
    std::atomic<std::int64_t> medium_spillway_size {0};
    std::atomic<std::int64_t> large_spillway_size {0};
    std::atomic<std::uint64_t> mkdir_requests_issued {0};
    std::atomic<std::uint64_t> mkdir_completions_ack {0};
    std::atomic<std::uint64_t> mkdir_wait_ns {0};
    std::atomic<std::uint64_t> rx_buffers_received {0};
    std::atomic<std::uint64_t> rx_bytes_received {0};
    std::atomic<std::uint64_t> large_to_medium_steals {0};
    std::atomic<std::uint64_t> large_to_small_steals {0};
    std::atomic<std::uint64_t> medium_to_small_steals {0};
    std::atomic<std::uint64_t> medium_to_large_steals {0};
};

void print_direct_data_buffer_write_stats(const DataReadBenchmarkStats& stats,
                                          DataReadFileQueue& file_queue,
                                          const DirectTargetWriterStats& writer) {
    const DataReadBenchmarkSnapshot snapshot = snapshot_data_read_stats(stats);
    const TargetWriterStats writer_stats = writer.snapshot();
    const double write_gbps = snapshot.elapsed_seconds > 0.0
                                  ? static_cast<double>(writer_stats.bytes_written) * 8.0 /
                                        snapshot.elapsed_seconds / 1'000'000'000.0
                                  : 0.0;
    const double write_files_per_second = snapshot.elapsed_seconds > 0.0
                                              ? static_cast<double>(writer_stats.files_written) /
                                                    snapshot.elapsed_seconds
                                              : 0.0;
    std::cerr << "data_write_stats read_gigabits_per_second=" << snapshot.gigabits_per_second
              << " read_files_per_second=" << snapshot.files_per_second
              << " write_gigabits_per_second=" << write_gbps
              << " write_files_per_second=" << write_files_per_second
              << " files_found=" << snapshot.files_found
              << " files_read=" << snapshot.files_read
              << " files_written=" << writer_stats.files_written
              << " files_failed=" << snapshot.files_failed
              << " write_failed=" << writer_stats.files_failed
              << " bytes_read=" << snapshot.bytes_read
              << " bytes_written=" << writer_stats.bytes_written
              << " queued_files=" << queued_data_read_files(file_queue)
              << " data_queue_depth=0"
              << " data_queue_high_watermark=0"
              << " data_queue_shards=0"
              << " elapsed_seconds=" << snapshot.elapsed_seconds << '\n';
}

void run_direct_data_buffer_write_stats_printer(DataReadBenchmarkStats& stats,
                                                DataReadFileQueue& file_queue,
                                                const DirectTargetWriterStats& writer) {
    const auto interval = std::chrono::seconds(stats.print_interval_seconds);
    std::unique_lock<std::mutex> lock(stats.printer_mutex);
    while (true) {
        if (stats.printer_cv.wait_for(lock, interval, [&stats] {
                return stats.printer_done.load(std::memory_order_relaxed);
            })) {
            break;
        }
        std::lock_guard<std::mutex> print_lock(stats.print_mutex);
        stats.last_print_at = std::chrono::steady_clock::now();
        print_direct_data_buffer_write_stats(stats, file_queue, writer);
    }
}

void print_mixed_folder_ready_write_stats(const DataReadBenchmarkStats& stats,
                                          DataReadFileQueue& small_queue,
                                          DataReadFileQueue& medium_queue,
                                          DataReadFileQueue& large_queue,
                                          ReadyFileSpillway& medium_spillway,
                                          ReadyFileSpillway& large_spillway,
                                          const std::atomic<std::uint64_t>& current_small_iops_x100,
                                          const std::atomic<std::uint32_t>& bulk_pacing_us,
                                          const DirectTargetWriterStats& small_writer,
                                          const TargetDataWriterJob& medium_writer,
                                          const TargetDataWriterJob& large_writer) {
    const DataReadBenchmarkSnapshot snapshot = snapshot_data_read_stats(stats);
    const TargetWriterStats small_stats = small_writer.snapshot();
    const TargetWriterStats medium_stats = medium_writer.stats();
    const TargetWriterStats large_stats = large_writer.stats();
    const std::uint64_t total_bytes_written =
        small_stats.bytes_written + medium_stats.bytes_written + large_stats.bytes_written;
    const std::uint64_t total_files_written =
        small_stats.files_written + medium_stats.files_written + large_stats.files_written;
    const double elapsed = snapshot.elapsed_seconds;
    const double small_write_gbps = elapsed > 0.0
                                        ? static_cast<double>(small_stats.bytes_written) * 8.0 /
                                              elapsed / 1'000'000'000.0
                                        : 0.0;
    const double large_write_gbps = elapsed > 0.0
                                        ? static_cast<double>(large_stats.bytes_written) * 8.0 /
                                              elapsed / 1'000'000'000.0
                                        : 0.0;
    const double medium_write_gbps = elapsed > 0.0
                                         ? static_cast<double>(medium_stats.bytes_written) * 8.0 /
                                               elapsed / 1'000'000'000.0
                                         : 0.0;
    const double total_write_gbps = elapsed > 0.0
                                        ? static_cast<double>(total_bytes_written) * 8.0 /
                                              elapsed / 1'000'000'000.0
                                        : 0.0;
    const double small_write_files_per_second = elapsed > 0.0
                                                    ? static_cast<double>(small_stats.files_written) / elapsed
                                                    : 0.0;
    const double large_write_files_per_second = elapsed > 0.0
                                                    ? static_cast<double>(large_stats.files_written) / elapsed
                                                    : 0.0;
    const double medium_write_files_per_second = elapsed > 0.0
                                                     ? static_cast<double>(medium_stats.files_written) / elapsed
                                                     : 0.0;
    const double total_write_files_per_second = elapsed > 0.0
                                                    ? static_cast<double>(total_files_written) / elapsed
                                                    : 0.0;
    std::cerr << "mixed_data_write_stats total_write_gigabits_per_second=" << total_write_gbps
              << " small_write_files_per_second=" << small_write_files_per_second
              << " small_write_gigabits_per_second=" << small_write_gbps
              << " medium_write_gigabits_per_second=" << medium_write_gbps
              << " medium_write_files_per_second=" << medium_write_files_per_second
              << " large_write_gigabits_per_second=" << large_write_gbps
              << " large_write_files_per_second=" << large_write_files_per_second
              << " total_write_files_per_second=" << total_write_files_per_second
              << " small_files_found=" << snapshot.small_files_found
              << " large_files_found=" << snapshot.large_files_found
              << " small_files_read=" << snapshot.small_files_read
              << " large_files_read=" << snapshot.large_files_read
              << " small_files_written=" << small_stats.files_written
              << " medium_files_written=" << medium_stats.files_written
              << " large_files_written=" << large_stats.files_written
              << " files_failed=" << snapshot.files_failed
              << " small_write_failed=" << small_stats.files_failed
              << " medium_write_failed=" << medium_stats.files_failed
              << " large_write_failed=" << large_stats.files_failed
              << " small_bytes_written=" << small_stats.bytes_written
              << " medium_bytes_written=" << medium_stats.bytes_written
              << " large_bytes_written=" << large_stats.bytes_written
              << " queued_small_files=" << queued_data_read_files(small_queue)
              << " queued_medium_files=" << queued_data_read_files(medium_queue)
              << " queued_large_files=" << queued_data_read_files(large_queue)
              << " spill_medium_files=" << queued_ready_file_spillway(medium_spillway)
              << " spill_large_files=" << queued_ready_file_spillway(large_spillway)
              << " current_small_iops="
              << (static_cast<double>(current_small_iops_x100.load(std::memory_order_relaxed)) / 100.0)
              << " bulk_pacing_us=" << bulk_pacing_us.load(std::memory_order_relaxed)
              << " elapsed_seconds=" << elapsed << '\n';
}

void run_mixed_folder_ready_write_stats_printer(DataReadBenchmarkStats& stats,
                                                DataReadFileQueue& small_queue,
                                                DataReadFileQueue& medium_queue,
                                                DataReadFileQueue& large_queue,
                                                ReadyFileSpillway& medium_spillway,
                                                ReadyFileSpillway& large_spillway,
                                                const std::atomic<std::uint64_t>& current_small_iops_x100,
                                                const std::atomic<std::uint32_t>& bulk_pacing_us,
                                                const DirectTargetWriterStats& small_writer,
                                                const TargetDataWriterJob& medium_writer,
                                                const TargetDataWriterJob& large_writer) {
    const auto interval = std::chrono::seconds(stats.print_interval_seconds);
    std::unique_lock<std::mutex> lock(stats.printer_mutex);
    while (true) {
        if (stats.printer_cv.wait_for(lock, interval, [&stats] {
                return stats.printer_done.load(std::memory_order_relaxed);
            })) {
            break;
        }
        std::lock_guard<std::mutex> print_lock(stats.print_mutex);
        stats.last_print_at = std::chrono::steady_clock::now();
        print_mixed_folder_ready_write_stats(stats,
                                             small_queue,
                                             medium_queue,
                                             large_queue,
                                             medium_spillway,
                                             large_spillway,
                                             current_small_iops_x100,
                                             bulk_pacing_us,
                                             small_writer,
                                             medium_writer,
                                             large_writer);
    }
}

void print_folder_ready_discard_stats(const DataReadBenchmarkStats& stats,
                                      FolderReadyBatchQueue& folder_batch_queue,
                                      DataReadFileQueue& ready_file_queue,
                                      const std::atomic<std::uint64_t>& folders_created,
                                      const DirectTargetWriterStats& discard_stats) {
    const DataReadBenchmarkSnapshot snapshot = snapshot_data_read_stats(stats);
    const TargetWriterStats sink = discard_stats.snapshot();
    const double sink_files_per_second = snapshot.elapsed_seconds > 0.0
                                             ? static_cast<double>(sink.files_written) / snapshot.elapsed_seconds
                                             : 0.0;
    const double sink_gbps = snapshot.elapsed_seconds > 0.0
                                 ? static_cast<double>(sink.bytes_written) * 8.0 /
                                       snapshot.elapsed_seconds / 1'000'000'000.0
                                 : 0.0;
    const std::size_t folder_batches = queued_folder_ready_batches(folder_batch_queue);
    std::cerr << "folder_ready_discard_stats files_found=" << snapshot.files_found
              << " folders_found=" << snapshot.folders_found
              << " folders_created=" << folders_created.load(std::memory_order_relaxed)
              << " files_released=" << snapshot.files_read
              << " files_discarded=" << sink.files_written
              << " files_per_second=" << sink_files_per_second
              << " gigabits_per_second=" << sink_gbps
              << " bytes_discarded=" << sink.bytes_written
              << " logical_size_bytes=" << snapshot.logical_size_bytes
              << " folder_batch_queue_depth=" << folder_batches
              << " ready_file_queue_depth=" << queued_data_read_files(ready_file_queue)
              << " elapsed_seconds=" << snapshot.elapsed_seconds << '\n';
}

void run_folder_ready_discard_stats_printer(DataReadBenchmarkStats& stats,
                                            FolderReadyBatchQueue& folder_batch_queue,
                                            DataReadFileQueue& ready_file_queue,
                                            const std::atomic<std::uint64_t>& folders_created,
                                            const DirectTargetWriterStats& discard_stats) {
    const auto interval = std::chrono::seconds(stats.print_interval_seconds);
    std::unique_lock<std::mutex> lock(stats.printer_mutex);
    while (true) {
        if (stats.printer_cv.wait_for(lock, interval, [&stats] {
                return stats.printer_done.load(std::memory_order_relaxed);
            })) {
            break;
        }
        std::lock_guard<std::mutex> print_lock(stats.print_mutex);
        stats.last_print_at = std::chrono::steady_clock::now();
        print_folder_ready_discard_stats(stats,
                                         folder_batch_queue,
                                         ready_file_queue,
                                         folders_created,
                                         discard_stats);
    }
}

void print_nfs_open_stats(const DataReadBenchmarkStats& stats, DataReadFileQueue& file_queue) {
    const DataReadBenchmarkSnapshot snapshot = snapshot_data_read_stats(stats);
    const NfsAsyncCommandLatencySnapshot command_latency = snapshot_nfs_async_command_latency_metrics();
    const double files_per_second =
        snapshot.elapsed_seconds > 0.0 ? static_cast<double>(snapshot.files_read) / snapshot.elapsed_seconds : 0.0;
    const double avg_open_ms = command_latency.open_completed != 0U
                                   ? static_cast<double>(command_latency.open_latency_ns) /
                                         static_cast<double>(command_latency.open_completed) / 1'000'000.0
                                   : 0.0;
    const double avg_close_ms = command_latency.close_completed != 0U
                                    ? static_cast<double>(command_latency.close_latency_ns) /
                                          static_cast<double>(command_latency.close_completed) / 1'000'000.0
                                    : 0.0;
    std::cerr << "nfs_open_stats files_per_second=" << files_per_second
              << " files_opened=" << snapshot.files_read
              << " files_failed=" << snapshot.files_failed
              << " files_found=" << snapshot.files_found
              << " folders_found=" << snapshot.folders_found
              << " logical_size_bytes=" << snapshot.logical_size_bytes
              << " async_open_completed=" << command_latency.open_completed
              << " async_open_failed=" << command_latency.open_failed
              << " async_open_avg_latency_ms=" << avg_open_ms
              << " async_open_max_latency_ms=" << static_cast<double>(command_latency.open_max_latency_ns) / 1'000'000.0
              << " async_close_completed=" << command_latency.close_completed
              << " async_close_failed=" << command_latency.close_failed
              << " async_close_avg_latency_ms=" << avg_close_ms
              << " async_close_max_latency_ms=" << static_cast<double>(command_latency.close_max_latency_ns) / 1'000'000.0
              << " queued_files=" << queued_data_read_files(file_queue)
              << " elapsed_seconds=" << snapshot.elapsed_seconds << '\n';
}

void run_nfs_open_stats_printer(DataReadBenchmarkStats& stats, DataReadFileQueue& file_queue) {
    const auto interval = std::chrono::seconds(stats.print_interval_seconds);
    std::unique_lock<std::mutex> lock(stats.printer_mutex);
    while (true) {
        if (stats.printer_cv.wait_for(lock, interval, [&stats] {
                return stats.printer_done.load(std::memory_order_relaxed);
            })) {
            break;
        }
        std::lock_guard<std::mutex> print_lock(stats.print_mutex);
        stats.last_print_at = std::chrono::steady_clock::now();
        print_nfs_open_stats(stats, file_queue);
    }
}

void print_data_hash_stats(const DataReadBenchmarkStats& stats,
                           DataReadFileQueue& file_queue,
                           const DataHasherJob& hasher,
                           const BufQueue& data_queue,
                           const BufQueue& hashed_queue) {
    const DataReadBenchmarkSnapshot read = snapshot_data_read_stats(stats);
    const DataHasherStats hash = hasher.stats();
    const double hash_bytes_per_second =
        read.elapsed_seconds > 0.0 ? static_cast<double>(hash.bytes_hashed) / read.elapsed_seconds : 0.0;
    const double hash_gigabits_per_second = hash_bytes_per_second * 8.0 / 1'000'000'000.0;
    std::cerr << "data_hash_stats read_bytes_per_second=" << read.bytes_per_second
              << " read_gigabits_per_second=" << read.gigabits_per_second
              << " hash_bytes_per_second=" << hash_bytes_per_second
              << " hash_gigabits_per_second=" << hash_gigabits_per_second
              << " bytes_read=" << read.bytes_read
              << " bytes_hashed=" << hash.bytes_hashed
              << " files_read=" << read.files_read
              << " files_failed=" << read.files_failed
              << " files_found=" << read.files_found
              << " folders_found=" << read.folders_found
              << " queued_files=" << queued_data_read_files(file_queue)
              << " data_queue_depth=" << data_queue.size()
              << " hashed_queue_depth=" << hashed_queue.size()
              << " hash_work_factor=" << hash.work_factor
              << " elapsed_seconds=" << read.elapsed_seconds << '\n';
}

void run_data_hash_stats_printer(DataReadBenchmarkStats& stats,
                                 DataReadFileQueue& file_queue,
                                 const DataHasherJob& hasher,
                                 const BufQueue& data_queue,
                                 const BufQueue& hashed_queue) {
    const auto interval = std::chrono::seconds(stats.print_interval_seconds);
    std::unique_lock<std::mutex> lock(stats.printer_mutex);
    while (true) {
        if (stats.printer_cv.wait_for(lock, interval, [&stats] {
                return stats.printer_done.load(std::memory_order_relaxed);
            })) {
            break;
        }
        std::lock_guard<std::mutex> print_lock(stats.print_mutex);
        stats.last_print_at = std::chrono::steady_clock::now();
        print_data_hash_stats(stats, file_queue, hasher, data_queue, hashed_queue);
    }
}

bool data_read_timer_expired(const DataReadFileQueue& queue) {
    return queue.stop_at.has_value() && std::chrono::steady_clock::now() >= *queue.stop_at;
}

void fail_data_file_work(DataReadFileQueue& queue, std::exception_ptr error = std::current_exception()) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.stop = true;
        if (!queue.error && error != nullptr) {
            queue.error = error;
        }
    }
    queue.cv_not_empty.notify_all();
    queue.cv_not_full.notify_all();
}

void request_data_file_stop(DataReadFileQueue& queue) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.stop = true;
        queue.files.clear();
    }
    queue.cv_not_empty.notify_all();
    queue.cv_not_full.notify_all();
}

bool enqueue_data_read_files(DataReadFileQueue& queue, std::vector<FileSpec>&& files) {
    for (auto& file : files) {
        std::unique_lock<std::mutex> lock(queue.mutex);
        const auto ready = [&queue]() {
            if (queue.stop || queue.error || data_read_timer_expired(queue)) {
                return true;
            }
            if (queue.wait_for_low_watermark) {
                return queue.files.size() <= queue.resume_entries;
            }
            return queue.files.size() < queue.max_entries;
        };
        if (queue.resume_entries != 0U && queue.files.size() >= queue.max_entries) {
            queue.wait_for_low_watermark = true;
        }
        if (queue.stop_at.has_value()) {
            queue.cv_not_full.wait_until(lock, *queue.stop_at, ready);
        } else {
            queue.cv_not_full.wait(lock, ready);
        }

        if (data_read_timer_expired(queue)) {
            queue.stop = true;
            queue.files.clear();
        }
        if (queue.stop || queue.error) {
            queue.cv_not_empty.notify_all();
            queue.cv_not_full.notify_all();
            return false;
        }

        queue.wait_for_low_watermark = false;
        queue.files.push_back(std::move(file));
        lock.unlock();
        queue.cv_not_empty.notify_one();
    }
    return true;
}

void fail_ready_file_spillway(ReadyFileSpillway& spillway, std::exception_ptr error = std::current_exception()) {
    {
        std::lock_guard<std::mutex> lock(spillway.mutex);
        spillway.error = error;
        spillway.stop = true;
        spillway.files.clear();
    }
    spillway.cv_not_empty.notify_all();
}

void request_ready_file_spillway_stop(ReadyFileSpillway& spillway) {
    {
        std::lock_guard<std::mutex> lock(spillway.mutex);
        spillway.stop = true;
        spillway.files.clear();
    }
    spillway.cv_not_empty.notify_all();
}

void mark_ready_file_spillway_input_done(ReadyFileSpillway& spillway) {
    {
        std::lock_guard<std::mutex> lock(spillway.mutex);
        spillway.input_done = true;
    }
    spillway.cv_not_empty.notify_all();
}

std::size_t queued_ready_file_spillway(ReadyFileSpillway& spillway) {
    std::lock_guard<std::mutex> lock(spillway.mutex);
    return spillway.files.size();
}

bool spill_ready_files(ReadyFileSpillway& spillway, std::vector<FileSpec>&& files) {
    if (files.empty()) {
        return true;
    }
    {
        std::lock_guard<std::mutex> lock(spillway.mutex);
        if (spillway.stop || spillway.error) {
            return false;
        }
        for (auto& file : files) {
            spillway.files.push_back(std::move(file));
        }
        spillway.high_watermark = std::max(spillway.high_watermark, spillway.files.size());
    }
    spillway.cv_not_empty.notify_one();
    return true;
}

std::vector<FileSpec> take_ready_file_spill_batch(ReadyFileSpillway& spillway, std::size_t max_files) {
    std::vector<FileSpec> batch;
    if (max_files == 0U) {
        return batch;
    }
    std::unique_lock<std::mutex> lock(spillway.mutex);
    spillway.cv_not_empty.wait(lock, [&spillway]() {
        return spillway.stop || spillway.error || !spillway.files.empty() || spillway.input_done;
    });
    if (spillway.stop || spillway.error || spillway.files.empty()) {
        return batch;
    }
    const std::size_t count = std::min(max_files, spillway.files.size());
    batch.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
        batch.push_back(std::move(spillway.files.front()));
        spillway.files.pop_front();
    }
    return batch;
}

void fail_target_buffer_spillway(TargetBufferSpillway& spillway,
                                 RawBufferPool& pool,
                                 std::exception_ptr error = std::current_exception()) {
    std::deque<BufferHandle> handles;
    {
        std::lock_guard<std::mutex> lock(spillway.mutex);
        spillway.error = error;
        spillway.stop = true;
        handles.swap(spillway.handles);
    }
    if (spillway.depth_counter != nullptr) {
        spillway.depth_counter->fetch_sub(static_cast<std::int64_t>(handles.size()), std::memory_order_relaxed);
    }
    for (const BufferHandle& handle : handles) {
        pool.release(handle);
    }
    spillway.cv_not_empty.notify_all();
}

void mark_target_buffer_spillway_input_done(TargetBufferSpillway& spillway) {
    {
        std::lock_guard<std::mutex> lock(spillway.mutex);
        spillway.input_done = true;
    }
    spillway.cv_not_empty.notify_all();
}

bool spill_target_buffer(TargetBufferSpillway& spillway, const BufferHandle& handle) {
    {
        std::lock_guard<std::mutex> lock(spillway.mutex);
        if (spillway.stop || spillway.error) {
            return false;
        }
        spillway.handles.push_back(handle);
        spillway.high_watermark = std::max(spillway.high_watermark, spillway.handles.size());
    }
    if (spillway.depth_counter != nullptr) {
        spillway.depth_counter->fetch_add(1, std::memory_order_relaxed);
    }
    spillway.cv_not_empty.notify_one();
    return true;
}

std::vector<BufferHandle> take_target_buffer_spill_batch(TargetBufferSpillway& spillway,
                                                         std::size_t max_handles) {
    std::vector<BufferHandle> batch;
    if (max_handles == 0U) {
        return batch;
    }
    std::unique_lock<std::mutex> lock(spillway.mutex);
    spillway.cv_not_empty.wait(lock, [&spillway]() {
        return spillway.stop || spillway.error || !spillway.handles.empty() || spillway.input_done;
    });
    if (spillway.stop || spillway.error || spillway.handles.empty()) {
        return batch;
    }
    const std::size_t count = std::min(max_handles, spillway.handles.size());
    batch.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
        batch.push_back(spillway.handles.front());
        spillway.handles.pop_front();
    }
    lock.unlock();
    if (spillway.depth_counter != nullptr) {
        spillway.depth_counter->fetch_sub(static_cast<std::int64_t>(count), std::memory_order_relaxed);
    }
    return batch;
}

bool try_enqueue_data_read_files_or_spill(DataReadFileQueue& queue,
                                          ReadyFileSpillway& spillway,
                                          std::vector<FileSpec>&& files) {
    if (files.empty()) {
        return true;
    }
    std::vector<FileSpec> overflow;
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        if (data_read_timer_expired(queue)) {
            queue.stop = true;
            queue.files.clear();
        }
        if (queue.stop || queue.error) {
            queue.cv_not_empty.notify_all();
            queue.cv_not_full.notify_all();
            return false;
        }
        for (auto& file : files) {
            if (queue.files.size() < queue.max_entries) {
                queue.files.push_back(std::move(file));
            } else {
                overflow.push_back(std::move(file));
            }
        }
    }
    queue.cv_not_empty.notify_all();
    return spill_ready_files(spillway, std::move(overflow));
}

void classify_ready_files(std::uint64_t small_threshold,
                          std::uint64_t medium_threshold,
                          std::vector<FileSpec>&& files,
                          std::vector<FileSpec>& small_files,
                          std::vector<FileSpec>& medium_files,
                          std::vector<FileSpec>& large_files,
                          std::uint64_t& small_bytes,
                          std::uint64_t& medium_bytes,
                          std::uint64_t& large_bytes) {
    small_files.reserve(files.size());
    medium_files.reserve(files.size());
    large_files.reserve(files.size());
    for (auto& file : files) {
        const std::uint64_t logical_size = file.declared_size != 0U ? file.declared_size : file.content.size();
        if (logical_size <= small_threshold) {
            small_bytes += logical_size;
            small_files.push_back(std::move(file));
        } else if (logical_size <= medium_threshold) {
            medium_bytes += logical_size;
            medium_files.push_back(std::move(file));
        } else {
            large_bytes += logical_size;
            large_files.push_back(std::move(file));
        }
    }
}

std::optional<FileSpec> take_data_file_work(DataReadFileQueue& queue) {
    std::unique_lock<std::mutex> lock(queue.mutex);
    const auto ready = [&queue]() {
        return queue.stop || queue.error || !queue.files.empty() || queue.input_done || data_read_timer_expired(queue);
    };
    if (queue.stop_at.has_value()) {
        queue.cv_not_empty.wait_until(lock, *queue.stop_at, ready);
    } else {
        queue.cv_not_empty.wait(lock, ready);
    }

    if (data_read_timer_expired(queue)) {
        queue.stop = true;
        queue.files.clear();
    }
    if (queue.stop || queue.error || queue.files.empty()) {
        return std::nullopt;
    }

    FileSpec file = std::move(queue.files.front());
    queue.files.pop_front();
    lock.unlock();
    queue.cv_not_full.notify_one();
    return file;
}

std::optional<FileSpec> try_take_data_file_work(DataReadFileQueue& queue) {
    std::lock_guard<std::mutex> lock(queue.mutex);
    if (data_read_timer_expired(queue)) {
        queue.stop = true;
        queue.files.clear();
    }
    if (queue.stop || queue.error || queue.files.empty()) {
        return std::nullopt;
    }
    FileSpec file = std::move(queue.files.front());
    queue.files.pop_front();
    queue.cv_not_full.notify_one();
    return file;
}

std::vector<FileSpec> take_data_file_work_batch(DataReadFileQueue& queue, std::size_t max_files) {
    std::vector<FileSpec> batch;
    if (max_files == 0U) {
        return batch;
    }

    std::unique_lock<std::mutex> lock(queue.mutex);
    const auto ready = [&queue]() {
        return queue.stop || queue.error || !queue.files.empty() || queue.input_done || data_read_timer_expired(queue);
    };
    if (queue.stop_at.has_value()) {
        queue.cv_not_empty.wait_until(lock, *queue.stop_at, ready);
    } else {
        queue.cv_not_empty.wait(lock, ready);
    }

    if (data_read_timer_expired(queue)) {
        queue.stop = true;
        queue.files.clear();
    }
    if (queue.stop || queue.error || queue.files.empty()) {
        return batch;
    }

    const std::size_t count = std::min(max_files, queue.files.size());
    batch.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
        batch.push_back(std::move(queue.files.front()));
        queue.files.pop_front();
    }
    const bool wake_producer = !queue.wait_for_low_watermark || queue.files.size() <= queue.resume_entries;
    lock.unlock();
    if (wake_producer) {
        queue.cv_not_full.notify_all();
    }
    return batch;
}

void mark_data_file_input_done(DataReadFileQueue& queue) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.input_done = true;
    }
    queue.cv_not_empty.notify_all();
    queue.cv_not_full.notify_all();
}

void record_data_read_metadata_batch(bool recursive,
                                     FlatMetadataWorkQueue& folder_queue,
                                     DataReadFileQueue& file_queue,
                                     DataReadBenchmarkStats& stats,
                                     FlatFolderScanBatch batch,
                                     std::uint64_t min_file_size_bytes,
                                     std::uint64_t max_file_size_bytes,
                                     RawBufferPool* target_metadata_pool = nullptr,
                                     BufQueue* target_metadata_queue = nullptr) {
    if (batch.failed) {
        std::cerr << "metadata scan skipped folder '"
                  << (batch.folder.rel_path.empty() ? "/" : batch.folder.rel_path)
                  << "': " << (batch.error.empty() ? "unknown error" : batch.error) << '\n';
        if (batch.folder.rel_path.empty()) {
            const std::string message =
                batch.error.empty() ? "failed to scan root metadata folder" : batch.error;
            const auto error = std::make_exception_ptr(std::runtime_error(message));
            fail_flat_folder_work(folder_queue, error);
            fail_data_file_work(file_queue, error);
            return;
        }
        if (batch.complete) {
            finish_flat_folder_work(folder_queue);
        }
        return;
    }

    std::vector<FileSpec> files_to_read;
    files_to_read.reserve(batch.files.size());
    std::uint64_t logical_size_bytes = 0;
    for (auto& file : batch.files) {
        const std::uint64_t logical_size = file.declared_size != 0 ? file.declared_size : file.content.size();
        if (min_file_size_bytes != 0U && logical_size < min_file_size_bytes) {
            continue;
        }
        if (max_file_size_bytes != 0U && logical_size > max_file_size_bytes) {
            continue;
        }
        logical_size_bytes += logical_size;
        files_to_read.push_back(std::move(file));
    }

    std::vector<FileSpec> child_work;
    if (recursive && !flat_metadata_scan_should_stop(folder_queue)) {
        child_work.reserve(batch.directories.size());
        for (auto& directory : batch.directories) {
            directory.rel_path = normalize_path(directory.rel_path);
            child_work.push_back(directory);
        }
    }

    record_data_read_metadata(stats, files_to_read.size(), batch.directories.size(), logical_size_bytes);
    if (target_metadata_pool != nullptr &&
        target_metadata_queue != nullptr &&
        (!batch.folder.rel_path.empty() || !batch.directories.empty())) {
        std::vector<FileSpec> target_directories;
        target_directories.reserve(batch.directories.size() + (batch.folder.rel_path.empty() ? 0U : 1U));
        if (!batch.folder.rel_path.empty()) {
            target_directories.push_back(batch.folder);
        }
        target_directories.insert(target_directories.end(), batch.directories.begin(), batch.directories.end());
        std::uint32_t sequence = 0;
        std::optional<BufferHandle> handle;
        const auto flush_current = [&]() {
            if (!handle.has_value()) {
                return;
            }
            if (!target_metadata_queue->push_wait(*handle)) {
                target_metadata_pool->release(*handle);
            }
            handle.reset();
        };
        const auto start_buffer = [&]() {
            handle = target_metadata_pool->acquire_wait();
            MetadataBatchBuffer& buffer = metadata_batch_buffer(*target_metadata_pool, *handle);
            reset_flat_folder_buffer(buffer,
                                     batch.folder,
                                     sequence++,
                                     false,
                                     false,
                                     {},
                                     0,
                                     target_directories.size(),
                                     0,
                                     0,
                                     0,
                                     0);
        };
        start_buffer();
        for (const FileSpec& directory : target_directories) {
            MetadataBatchBuffer& buffer = metadata_batch_buffer(*target_metadata_pool, *handle);
            if (append_flat_folder_folder(buffer, directory, "size")) {
                continue;
            }
            flush_current();
            start_buffer();
            MetadataBatchBuffer& next_buffer = metadata_batch_buffer(*target_metadata_pool, *handle);
            if (!append_flat_folder_folder(next_buffer, directory, "size")) {
                target_metadata_pool->release(*handle);
                handle.reset();
                throw std::runtime_error("target directory metadata record exceeds buffer capacity");
            }
        }
        flush_current();
    }
    if (!enqueue_data_read_files(file_queue, std::move(files_to_read))) {
        if (batch.complete) {
            finish_flat_folder_work(folder_queue);
        }
        return;
    }
    enqueue_flat_folder_work(folder_queue, std::move(child_work));
    if (batch.complete) {
        finish_flat_folder_work(folder_queue);
    }
}

void record_data_read_metadata_batch_laned(bool recursive,
                                           FlatMetadataWorkQueue& folder_queue,
                                           std::vector<std::unique_ptr<DataReadFileQueue>>& lane_file_queues,
                                           DataReadBenchmarkStats& stats,
                                           FlatFolderScanBatch batch,
                                           std::uint64_t min_file_size_bytes,
                                           std::uint64_t max_file_size_bytes) {
    const auto fail_all_lanes = [&](std::exception_ptr error) {
        for (auto& queue : lane_file_queues) {
            fail_data_file_work(*queue, error);
        }
    };

    if (batch.failed) {
        std::cerr << "metadata scan skipped folder '"
                  << (batch.folder.rel_path.empty() ? "/" : batch.folder.rel_path)
                  << "': " << (batch.error.empty() ? "unknown error" : batch.error) << '\n';
        if (batch.folder.rel_path.empty()) {
            const std::string message =
                batch.error.empty() ? "failed to scan root metadata folder" : batch.error;
            const auto error = std::make_exception_ptr(std::runtime_error(message));
            fail_flat_folder_work(folder_queue, error);
            fail_all_lanes(error);
            return;
        }
        if (batch.complete) {
            finish_flat_folder_work(folder_queue);
        }
        return;
    }

    const std::size_t lanes = std::max<std::size_t>(1U, lane_file_queues.size());
    std::vector<std::vector<FileSpec>> files_by_lane(lanes);
    std::uint64_t logical_size_bytes = 0;
    for (auto& file : batch.files) {
        const std::uint64_t logical_size = file.declared_size != 0 ? file.declared_size : file.content.size();
        if (min_file_size_bytes != 0U && logical_size < min_file_size_bytes) {
            continue;
        }
        if (max_file_size_bytes != 0U && logical_size > max_file_size_bytes) {
            continue;
        }
        logical_size_bytes += logical_size;
        const std::string parent = parent_path(file.rel_path);
        const std::uint64_t key =
            logical_size <= kSmallFileThreshold && !parent.empty()
                ? hash64(parent)
                : hash64(file.rel_path);
        files_by_lane[static_cast<std::size_t>(key % lanes)].push_back(std::move(file));
    }

    std::vector<FileSpec> child_work;
    if (recursive && !flat_metadata_scan_should_stop(folder_queue)) {
        child_work.reserve(batch.directories.size());
        for (auto& directory : batch.directories) {
            directory.rel_path = normalize_path(directory.rel_path);
            child_work.push_back(directory);
        }
    }

    record_data_read_metadata(stats, batch.files.size(), batch.directories.size(), logical_size_bytes);
    for (std::size_t lane = 0; lane < lanes; ++lane) {
        if (!enqueue_data_read_files(*lane_file_queues[lane], std::move(files_by_lane[lane]))) {
            if (batch.complete) {
                finish_flat_folder_work(folder_queue);
            }
            return;
        }
    }
    enqueue_flat_folder_work(folder_queue, std::move(child_work));
    if (batch.complete) {
        finish_flat_folder_work(folder_queue);
    }
}

void record_folder_ready_metadata_batch(bool recursive,
                                        FlatMetadataWorkQueue& folder_queue,
                                        FolderReadyBatchQueue& folder_batch_queue,
                                        DataReadFileQueue& ready_file_queue,
                                        DataReadBenchmarkStats& stats,
                                        FlatFolderScanBatch batch,
                                        std::uint64_t min_file_size_bytes,
                                        std::uint64_t max_file_size_bytes) {
    if (batch.failed) {
        std::cerr << "metadata scan skipped folder '"
                  << (batch.folder.rel_path.empty() ? "/" : batch.folder.rel_path)
                  << "': " << (batch.error.empty() ? "unknown error" : batch.error) << '\n';
        if (batch.folder.rel_path.empty()) {
            const std::string message =
                batch.error.empty() ? "failed to scan root metadata folder" : batch.error;
            const auto error = std::make_exception_ptr(std::runtime_error(message));
            fail_flat_folder_work(folder_queue, error);
            fail_folder_ready_work(folder_batch_queue, error);
            fail_data_file_work(ready_file_queue, error);
            return;
        }
        if (batch.complete) {
            finish_flat_folder_work(folder_queue);
        }
        return;
    }

    FolderReadyFileBatch folder_batch;
    folder_batch.folder = batch.folder;
    folder_batch.directories.reserve(batch.directories.size() + (batch.folder.rel_path.empty() ? 0U : 1U));
    if (!batch.folder.rel_path.empty()) {
        folder_batch.directories.push_back(batch.folder);
    }
    for (auto& directory : batch.directories) {
        folder_batch.directories.push_back(directory);
    }

    folder_batch.files.reserve(batch.files.size());
    for (auto& file : batch.files) {
        const std::uint64_t logical_size = file.declared_size != 0 ? file.declared_size : file.content.size();
        if (min_file_size_bytes != 0U && logical_size < min_file_size_bytes) {
            continue;
        }
        if (max_file_size_bytes != 0U && logical_size > max_file_size_bytes) {
            continue;
        }
        folder_batch.logical_size_bytes += logical_size;
        folder_batch.files.push_back(std::move(file));
    }

    std::vector<FileSpec> child_work;
    if (recursive && !flat_metadata_scan_should_stop(folder_queue)) {
        child_work.reserve(batch.directories.size());
        for (auto& directory : batch.directories) {
            directory.rel_path = normalize_path(directory.rel_path);
            child_work.push_back(directory);
        }
    }

    record_data_read_metadata(stats,
                              folder_batch.files.size(),
                              batch.directories.size(),
                              folder_batch.logical_size_bytes);
    if (!folder_batch.files.empty() || !folder_batch.directories.empty()) {
        if (!enqueue_folder_ready_batch(folder_batch_queue, std::move(folder_batch))) {
            if (batch.complete) {
                finish_flat_folder_work(folder_queue);
            }
            return;
        }
    }
    enqueue_flat_folder_work(folder_queue, std::move(child_work));
    if (batch.complete) {
        finish_flat_folder_work(folder_queue);
    }
}

void record_split_data_read_metadata_batch(bool recursive,
                                           FlatMetadataWorkQueue& folder_queue,
                                           SplitDataReadFileQueues& file_queues,
                                           DataReadBenchmarkStats& stats,
                                           FlatFolderScanBatch batch) {
    if (batch.failed) {
        std::cerr << "metadata scan skipped folder '"
                  << (batch.folder.rel_path.empty() ? "/" : batch.folder.rel_path)
                  << "': " << (batch.error.empty() ? "unknown error" : batch.error) << '\n';
        if (batch.folder.rel_path.empty()) {
            const std::string message =
                batch.error.empty() ? "failed to scan root metadata folder" : batch.error;
            const auto error = std::make_exception_ptr(std::runtime_error(message));
            fail_flat_folder_work(folder_queue, error);
            fail_data_file_work(file_queues.small, error);
            fail_data_file_work(file_queues.large, error);
            return;
        }
        if (batch.complete) {
            finish_flat_folder_work(folder_queue);
        }
        return;
    }

    std::vector<FileSpec> small_files;
    std::vector<FileSpec> large_files;
    small_files.reserve(batch.files.size());
    large_files.reserve(batch.files.size());
    std::uint64_t logical_size_bytes = 0;
    std::uint64_t small_logical_size_bytes = 0;
    std::uint64_t large_logical_size_bytes = 0;
    for (auto& file : batch.files) {
        const std::uint64_t logical_size = file.declared_size != 0 ? file.declared_size : file.content.size();
        logical_size_bytes += logical_size;
        if (logical_size <= file_queues.small_file_threshold) {
            small_logical_size_bytes += logical_size;
            small_files.push_back(std::move(file));
        } else {
            large_logical_size_bytes += logical_size;
            large_files.push_back(std::move(file));
        }
    }

    std::vector<FileSpec> child_work;
    if (recursive && !flat_metadata_scan_should_stop(folder_queue)) {
        child_work.reserve(batch.directories.size());
        for (auto& directory : batch.directories) {
            directory.rel_path = normalize_path(directory.rel_path);
            child_work.push_back(directory);
        }
    }

    record_data_read_metadata(stats, small_files.size() + large_files.size(),
                              batch.directories.size(), logical_size_bytes);
    stats.small_files_found.fetch_add(small_files.size(), std::memory_order_relaxed);
    stats.large_files_found.fetch_add(large_files.size(), std::memory_order_relaxed);
    stats.small_logical_size_bytes.fetch_add(small_logical_size_bytes, std::memory_order_relaxed);
    stats.large_logical_size_bytes.fetch_add(large_logical_size_bytes, std::memory_order_relaxed);

    if (!enqueue_data_read_files(file_queues.small, std::move(small_files)) ||
        !enqueue_data_read_files(file_queues.large, std::move(large_files))) {
        if (batch.complete) {
            finish_flat_folder_work(folder_queue);
        }
        return;
    }
    enqueue_flat_folder_work(folder_queue, std::move(child_work));
    if (batch.complete) {
        finish_flat_folder_work(folder_queue);
    }
}

void record_filtered_split_data_read_metadata_batch(bool recursive,
                                                    FlatMetadataWorkQueue& folder_queue,
                                                    DataReadFileQueue& file_queue,
                                                    std::uint64_t small_file_threshold,
                                                    SplitDataReadRoute route,
                                                    DataReadBenchmarkStats& stats,
                                                    FlatFolderScanBatch batch) {
    if (batch.failed) {
        std::cerr << "metadata scan skipped folder '"
                  << (batch.folder.rel_path.empty() ? "/" : batch.folder.rel_path)
                  << "': " << (batch.error.empty() ? "unknown error" : batch.error) << '\n';
        if (batch.folder.rel_path.empty()) {
            const std::string message =
                batch.error.empty() ? "failed to scan root metadata folder" : batch.error;
            const auto error = std::make_exception_ptr(std::runtime_error(message));
            fail_flat_folder_work(folder_queue, error);
            fail_data_file_work(file_queue, error);
            return;
        }
        if (batch.complete) {
            finish_flat_folder_work(folder_queue);
        }
        return;
    }

    std::vector<FileSpec> files_to_read;
    files_to_read.reserve(batch.files.size());
    std::uint64_t logical_size_bytes = 0;
    for (auto& file : batch.files) {
        const std::uint64_t logical_size = file.declared_size != 0 ? file.declared_size : file.content.size();
        const bool small_file = logical_size <= small_file_threshold;
        if ((route == SplitDataReadRoute::Small && small_file) ||
            (route == SplitDataReadRoute::Large && !small_file)) {
            logical_size_bytes += logical_size;
            files_to_read.push_back(std::move(file));
        }
    }

    std::vector<FileSpec> child_work;
    if (recursive && !flat_metadata_scan_should_stop(folder_queue)) {
        child_work.reserve(batch.directories.size());
        for (auto& directory : batch.directories) {
            directory.rel_path = normalize_path(directory.rel_path);
            child_work.push_back(directory);
        }
    }

    record_data_read_metadata(stats, files_to_read.size(), batch.directories.size(), logical_size_bytes);
    if (route == SplitDataReadRoute::Small) {
        stats.small_files_found.fetch_add(files_to_read.size(), std::memory_order_relaxed);
        stats.small_logical_size_bytes.fetch_add(logical_size_bytes, std::memory_order_relaxed);
    } else {
        stats.large_files_found.fetch_add(files_to_read.size(), std::memory_order_relaxed);
        stats.large_logical_size_bytes.fetch_add(logical_size_bytes, std::memory_order_relaxed);
    }

    if (!enqueue_data_read_files(file_queue, std::move(files_to_read))) {
        if (batch.complete) {
            finish_flat_folder_work(folder_queue);
        }
        return;
    }
    enqueue_flat_folder_work(folder_queue, std::move(child_work));
    if (batch.complete) {
        finish_flat_folder_work(folder_queue);
    }
}

void record_recon_metadata_batch(bool recursive,
                                 FlatMetadataWorkQueue& folder_queue,
                                 ReconScanStats& recon_stats,
                                 std::uint64_t small_file_threshold,
                                 std::uint64_t page_sleep_us,
                                 FlatFolderScanBatch batch) {
    if (batch.failed) {
        std::cerr << "metadata recon skipped folder '"
                  << (batch.folder.rel_path.empty() ? "/" : batch.folder.rel_path)
                  << "': " << (batch.error.empty() ? "unknown error" : batch.error) << '\n';
        if (batch.folder.rel_path.empty()) {
            const std::string message =
                batch.error.empty() ? "failed to scan root metadata folder" : batch.error;
            fail_flat_folder_work(folder_queue, std::make_exception_ptr(std::runtime_error(message)));
            return;
        }
        if (batch.complete) {
            finish_flat_folder_work(folder_queue);
        }
        if (page_sleep_us != 0U) {
            std::this_thread::sleep_for(std::chrono::microseconds(page_sleep_us));
        }
        return;
    }

    std::size_t small_files = 0;
    std::size_t large_files = 0;
    std::uint64_t logical_size_bytes = 0;
    std::uint64_t small_logical_size_bytes = 0;
    std::uint64_t large_logical_size_bytes = 0;
    for (const auto& file : batch.files) {
        const std::uint64_t logical_size = file.declared_size != 0 ? file.declared_size : file.content.size();
        logical_size_bytes += logical_size;
        if (logical_size <= small_file_threshold) {
            ++small_files;
            small_logical_size_bytes += logical_size;
        } else {
            ++large_files;
            large_logical_size_bytes += logical_size;
        }
    }

    std::vector<FileSpec> child_work;
    if (recursive && !flat_metadata_scan_should_stop(folder_queue)) {
        child_work.reserve(batch.directories.size());
        for (auto& directory : batch.directories) {
            directory.rel_path = normalize_path(directory.rel_path);
            child_work.push_back(directory);
        }
    }

    recon_stats.files_found.fetch_add(batch.files.size(), std::memory_order_relaxed);
    recon_stats.folders_found.fetch_add(batch.directories.size(), std::memory_order_relaxed);
    recon_stats.small_files_found.fetch_add(small_files, std::memory_order_relaxed);
    recon_stats.large_files_found.fetch_add(large_files, std::memory_order_relaxed);
    recon_stats.logical_size_bytes.fetch_add(logical_size_bytes, std::memory_order_relaxed);
    recon_stats.small_logical_size_bytes.fetch_add(small_logical_size_bytes, std::memory_order_relaxed);
    recon_stats.large_logical_size_bytes.fetch_add(large_logical_size_bytes, std::memory_order_relaxed);

    enqueue_flat_folder_work(folder_queue, std::move(child_work));
    if (batch.complete) {
        finish_flat_folder_work(folder_queue);
    }
    if (page_sleep_us != 0U) {
        std::this_thread::sleep_for(std::chrono::microseconds(page_sleep_us));
    }
}

void scan_data_read_metadata_worker(const std::string& source_root,
                                    bool recursive,
                                    std::size_t async_directory_depth,
                                    std::size_t readdirplus_page_bytes,
                                    std::uint64_t min_file_size_bytes,
                                    std::uint64_t max_file_size_bytes,
                                    FlatMetadataWorkQueue& folder_queue,
                                    DataReadFileQueue& file_queue,
                                    DataReadBenchmarkStats& stats,
                                    RawBufferPool* target_metadata_pool = nullptr,
                                    BufQueue* target_metadata_queue = nullptr) {
    auto backend = make_nfs_backend(source_root, kNfsEndpointAny, readdirplus_page_bytes);
    try {
        backend->scan_flat_folders_streaming(
            async_directory_depth,
            [&folder_queue](bool wait_for_work) {
                return take_flat_folder_work(folder_queue, wait_for_work);
            },
            [&folder_queue, &file_queue] {
                return flat_metadata_scan_should_stop(folder_queue) || data_read_timer_expired(file_queue);
            },
            [recursive,
             min_file_size_bytes,
             max_file_size_bytes,
             &folder_queue,
             &file_queue,
             &stats,
             target_metadata_pool,
             target_metadata_queue](FlatFolderScanBatch batch) {
                record_data_read_metadata_batch(recursive,
                                                folder_queue,
                                                file_queue,
                                                stats,
                                                std::move(batch),
                                                min_file_size_bytes,
                                                max_file_size_bytes,
                                                target_metadata_pool,
                                                target_metadata_queue);
            });
    } catch (...) {
        const std::exception_ptr error = std::current_exception();
        fail_flat_folder_work(folder_queue, error);
        fail_data_file_work(file_queue, error);
    }
}

void scan_data_read_metadata_worker_laned(const std::string& source_root,
                                          bool recursive,
                                          std::size_t async_directory_depth,
                                          std::size_t readdirplus_page_bytes,
                                          FlatMetadataWorkQueue& folder_queue,
                                          std::vector<std::unique_ptr<DataReadFileQueue>>& lane_file_queues,
                                          DataReadBenchmarkStats& stats) {
    auto backend = make_nfs_backend(source_root, kNfsEndpointAny, readdirplus_page_bytes);
    try {
        backend->scan_flat_folders_streaming(
            async_directory_depth,
            [&folder_queue](bool wait_for_work) {
                return take_flat_folder_work(folder_queue, wait_for_work);
            },
            [&folder_queue, &lane_file_queues] {
                bool expired = false;
                for (const auto& queue : lane_file_queues) {
                    expired = expired || data_read_timer_expired(*queue);
                }
                return flat_metadata_scan_should_stop(folder_queue) || expired;
            },
            [recursive, &folder_queue, &lane_file_queues, &stats](FlatFolderScanBatch batch) {
                record_data_read_metadata_batch_laned(recursive,
                                                      folder_queue,
                                                      lane_file_queues,
                                                      stats,
                                                      std::move(batch),
                                                      0,
                                                      0);
            });
    } catch (...) {
        const std::exception_ptr error = std::current_exception();
        fail_flat_folder_work(folder_queue, error);
        for (auto& queue : lane_file_queues) {
            fail_data_file_work(*queue, error);
        }
    }
}

void scan_folder_ready_metadata_worker(const std::string& source_root,
                                       bool recursive,
                                       std::size_t async_directory_depth,
                                       std::size_t readdirplus_page_bytes,
                                       std::uint64_t min_file_size_bytes,
                                       std::uint64_t max_file_size_bytes,
                                       FlatMetadataWorkQueue& folder_queue,
                                       FolderReadyBatchQueue& folder_batch_queue,
                                       DataReadFileQueue& ready_file_queue,
                                       DataReadBenchmarkStats& stats) {
    auto backend = make_nfs_backend(source_root, kNfsEndpointAny, readdirplus_page_bytes);
    try {
        backend->scan_flat_folders_streaming(
            async_directory_depth,
            [&folder_queue](bool wait_for_work) {
                return take_flat_folder_work(folder_queue, wait_for_work);
            },
            [&folder_queue, &folder_batch_queue, &ready_file_queue] {
                return flat_metadata_scan_should_stop(folder_queue) ||
                       folder_ready_timer_expired(folder_batch_queue) ||
                       data_read_timer_expired(ready_file_queue);
            },
            [recursive,
             min_file_size_bytes,
             max_file_size_bytes,
             &folder_queue,
             &folder_batch_queue,
             &ready_file_queue,
             &stats](FlatFolderScanBatch batch) {
                record_folder_ready_metadata_batch(recursive,
                                                   folder_queue,
                                                   folder_batch_queue,
                                                   ready_file_queue,
                                                   stats,
                                                   std::move(batch),
                                                   min_file_size_bytes,
                                                   max_file_size_bytes);
            });
    } catch (...) {
        const std::exception_ptr error = std::current_exception();
        fail_flat_folder_work(folder_queue, error);
        fail_folder_ready_work(folder_batch_queue, error);
        fail_data_file_work(ready_file_queue, error);
    }
}

void scan_split_data_read_metadata_worker(const std::string& source_root,
                                          bool recursive,
                                          std::size_t async_directory_depth,
                                          std::size_t readdirplus_page_bytes,
                                          FlatMetadataWorkQueue& folder_queue,
                                          SplitDataReadFileQueues& file_queues,
                                          DataReadBenchmarkStats& stats) {
    auto backend = make_nfs_backend(source_root, kNfsEndpointAny, readdirplus_page_bytes);
    try {
        backend->scan_flat_folders_streaming(
            async_directory_depth,
            [&folder_queue](bool wait_for_work) {
                return take_flat_folder_work(folder_queue, wait_for_work);
            },
            [&folder_queue, &file_queues] {
                return flat_metadata_scan_should_stop(folder_queue) ||
                       data_read_timer_expired(file_queues.small) ||
                       data_read_timer_expired(file_queues.large);
            },
            [recursive, &folder_queue, &file_queues, &stats](FlatFolderScanBatch batch) {
                record_split_data_read_metadata_batch(recursive,
                                                      folder_queue,
                                                      file_queues,
                                                      stats,
                                                      std::move(batch));
            });
    } catch (...) {
        const std::exception_ptr error = std::current_exception();
        fail_flat_folder_work(folder_queue, error);
        fail_data_file_work(file_queues.small, error);
        fail_data_file_work(file_queues.large, error);
    }
}

void scan_recon_metadata_worker(const std::string& source_root,
                                bool recursive,
                                std::size_t async_directory_depth,
                                std::size_t readdirplus_page_bytes,
                                FlatMetadataWorkQueue& folder_queue,
                                ReconScanStats& recon_stats,
                                std::uint64_t small_file_threshold,
                                std::uint64_t page_sleep_us,
                                ScannerCapacityControl* capacity_control = nullptr,
                                std::size_t worker_index = 0) {
    auto backend = make_nfs_backend(source_root, kNfsEndpointAny, readdirplus_page_bytes);
    try {
        backend->scan_flat_folders_streaming(
            std::max<std::size_t>(1, async_directory_depth),
            [&folder_queue, capacity_control, worker_index](bool wait_for_work) {
                if (!wait_until_scanner_worker_active(worker_index, capacity_control, [&]() {
                        return flat_metadata_scan_should_stop(folder_queue);
                    })) {
                    return std::optional<FileSpec> {};
                }
                return take_flat_folder_work(folder_queue, wait_for_work);
            },
            [&folder_queue] {
                return flat_metadata_scan_should_stop(folder_queue);
            },
            [recursive, &folder_queue, &recon_stats, small_file_threshold, page_sleep_us](FlatFolderScanBatch batch) {
                record_recon_metadata_batch(recursive,
                                            folder_queue,
                                            recon_stats,
                                            small_file_threshold,
                                            page_sleep_us,
                                            std::move(batch));
            });
        if (!folder_queue.error) {
            recon_stats.completed.store(true, std::memory_order_relaxed);
        }
    } catch (...) {
        fail_flat_folder_work(folder_queue, std::current_exception());
    }
}

void scan_filtered_split_data_read_metadata_worker(const std::string& source_root,
                                                   bool recursive,
                                                   std::size_t async_directory_depth,
                                                   std::size_t readdirplus_page_bytes,
                                                   FlatMetadataWorkQueue& folder_queue,
                                                   DataReadFileQueue& file_queue,
                                                   std::uint64_t small_file_threshold,
                                                   SplitDataReadRoute route,
                                                   DataReadBenchmarkStats& stats,
                                                   ScannerCapacityControl* capacity_control = nullptr,
                                                   std::size_t worker_index = 0) {
    auto backend = make_nfs_backend(source_root, kNfsEndpointAny, readdirplus_page_bytes);
    try {
        backend->scan_flat_folders_streaming(
            async_directory_depth,
            [&folder_queue, &file_queue, capacity_control, worker_index](bool wait_for_work) {
                if (!wait_until_scanner_worker_active(worker_index, capacity_control, [&]() {
                        return flat_metadata_scan_should_stop(folder_queue) ||
                               data_read_timer_expired(file_queue);
                    })) {
                    return std::optional<FileSpec> {};
                }
                return take_flat_folder_work(folder_queue, wait_for_work);
            },
            [&folder_queue, &file_queue] {
                return flat_metadata_scan_should_stop(folder_queue) ||
                       data_read_timer_expired(file_queue);
            },
            [recursive,
             &folder_queue,
             &file_queue,
             small_file_threshold,
             route,
             &stats](FlatFolderScanBatch batch) {
                record_filtered_split_data_read_metadata_batch(recursive,
                                                               folder_queue,
                                                               file_queue,
                                                               small_file_threshold,
                                                               route,
                                                               stats,
                                                               std::move(batch));
            });
    } catch (...) {
        const std::exception_ptr error = std::current_exception();
        fail_flat_folder_work(folder_queue, error);
        fail_data_file_work(file_queue, error);
    }
}

void nfs_open_only_worker(NfsDataReaderConfig data_config,
                          DataReadFileQueue& file_queue,
                          DataReadBenchmarkStats& stats) {
    NfsDataReader reader(std::move(data_config));
    while (auto file = take_data_file_work(file_queue)) {
        try {
            reader.open_close_file(*file);
            record_data_read_file(stats);
        } catch (...) {
            stats.files_failed.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

DataReadBenchmarkSnapshot run_parallel_nfs_open_scan(const NfsMetaReaderConfig& meta_config,
                                                     const NfsDataReaderConfig& data_config,
                                                     std::size_t max_files_queued,
                                                     double max_duration_seconds,
                                                     std::uint32_t stats_interval_seconds) {
    FlatMetadataWorkQueue folder_queue;
    folder_queue.folders.push_back(FileSpec{});
    DataReadFileQueue file_queue;
    file_queue.max_entries = std::max<std::size_t>(1, max_files_queued);

    DataReadBenchmarkStats stats;
    stats.print_interval_seconds = std::max<std::uint32_t>(1, stats_interval_seconds);
    stats.folders_found.store(1, std::memory_order_relaxed);
    reset_nfs_async_read_latency_metrics();

    const std::size_t open_threads = std::max<std::size_t>(1, data_config.data_reader_worker_count);
    const std::size_t metadata_threads = std::max<std::size_t>(1, meta_config.worker_count);

    const auto benchmark_started_at = std::chrono::steady_clock::now();
    stats.started_at = benchmark_started_at;
    stats.last_print_at = benchmark_started_at;
    if (max_duration_seconds > 0.0) {
        const auto stop_at = benchmark_started_at +
                             std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                 std::chrono::duration<double>(max_duration_seconds));
        folder_queue.stop_at = stop_at;
        file_queue.stop_at = stop_at;
    }

    std::mutex stop_timer_mutex;
    std::condition_variable stop_timer_cv;
    bool cancel_stop_timer = false;
    std::thread stop_timer;
    if (folder_queue.stop_at.has_value()) {
        const auto stop_at = *folder_queue.stop_at;
        stop_timer = std::thread([&]() {
            std::unique_lock<std::mutex> lock(stop_timer_mutex);
            const bool cancelled = stop_timer_cv.wait_until(lock, stop_at, [&]() {
                return cancel_stop_timer;
            });
            if (!cancelled) {
                request_flat_folder_stop(folder_queue);
                request_data_file_stop(file_queue);
            }
        });
    }

    std::thread stats_printer(run_nfs_open_stats_printer, std::ref(stats), std::ref(file_queue));

    std::vector<std::thread> open_workers;
    open_workers.reserve(open_threads);
    for (std::size_t index = 0; index < open_threads; ++index) {
        open_workers.emplace_back(nfs_open_only_worker, data_config, std::ref(file_queue), std::ref(stats));
    }

    std::vector<std::thread> metadata_workers;
    metadata_workers.reserve(metadata_threads);
    for (std::size_t index = 0; index < metadata_threads; ++index) {
        metadata_workers.emplace_back(scan_data_read_metadata_worker,
                                      meta_config.source_root,
                                      meta_config.recursive,
                                      std::max<std::size_t>(1, meta_config.async_directory_depth),
                                      meta_config.readdirplus_page_bytes,
                                      0,
                                      0,
                                      std::ref(folder_queue),
                                      std::ref(file_queue),
                                      std::ref(stats),
                                      nullptr,
                                      nullptr);
    }

    for (auto& worker : metadata_workers) {
        worker.join();
    }
    mark_data_file_input_done(file_queue);

    for (auto& worker : open_workers) {
        worker.join();
    }

    stats.printer_done.store(true, std::memory_order_relaxed);
    stats.printer_cv.notify_all();
    if (stats_printer.joinable()) {
        stats_printer.join();
    }

    {
        std::lock_guard<std::mutex> lock(stop_timer_mutex);
        cancel_stop_timer = true;
    }
    stop_timer_cv.notify_all();
    if (stop_timer.joinable()) {
        stop_timer.join();
    }

    if (folder_queue.error) {
        std::rethrow_exception(folder_queue.error);
    }
    if (file_queue.error) {
        std::rethrow_exception(file_queue.error);
    }

    return snapshot_data_read_stats(stats);
}

DataReadBenchmarkSnapshot run_parallel_data_read_scan(const NfsMetaReaderConfig& meta_config,
                                                      const NfsDataReaderConfig& data_config,
                                                      std::uint64_t max_file_size_bytes,
                                                      std::size_t max_files_queued,
                                                      std::size_t data_buffer_slots,
                                                      std::size_t data_queue_depth,
                                                      double max_duration_seconds,
                                                      std::uint32_t stats_interval_seconds,
                                                      const std::filesystem::path& status_socket_path) {
    FlatMetadataWorkQueue folder_queue;
    folder_queue.folders.push_back(FileSpec{});
    DataReadFileQueue file_queue;
    file_queue.max_entries = std::max<std::size_t>(1, max_files_queued);

    DataReadBenchmarkStats stats;
    stats.print_interval_seconds = std::max<std::uint32_t>(1, stats_interval_seconds);
    stats.folders_found.store(1, std::memory_order_relaxed);
    reset_nfs_async_read_latency_metrics();

    const std::size_t data_threads = std::max<std::size_t>(1, data_config.data_reader_worker_count);
    const std::size_t outstanding = std::max<std::size_t>(1, data_config.outstanding_requests);
    const std::size_t metadata_threads = std::max<std::size_t>(1, meta_config.worker_count);
    const std::size_t queue_depth =
        std::max<std::size_t>(1, data_queue_depth == 0 ? data_threads * outstanding * 2U : data_queue_depth);
    const std::size_t pool_slots =
        std::max<std::size_t>(data_threads * outstanding + queue_depth + 1U,
                              data_buffer_slots == 0 ? data_threads * outstanding * 3U + 1U
                                                     : data_buffer_slots);

    RawBufferPool data_pool = make_data_buffer_pool(pool_slots);
    BufferPoolRegistry registry;
    registry.register_pool(data_pool);
    BufQueue reader_to_discard(queue_depth);

    NfsDataBufferReaderJob data_reader_job(
        data_config,
        data_pool,
        reader_to_discard,
        [&file_queue]() {
            thread_local std::deque<FileSpec> worker_file_batch;
            if (worker_file_batch.empty()) {
                std::vector<FileSpec> next_batch = take_data_file_work_batch(file_queue, 128);
                for (auto& file : next_batch) {
                    worker_file_batch.push_back(std::move(file));
                }
            }
            if (worker_file_batch.empty()) {
                return std::optional<FileSpec> {};
            }
            FileSpec file = std::move(worker_file_batch.front());
            worker_file_batch.pop_front();
            return std::optional<FileSpec> {std::move(file)};
        },
        [&file_queue]() {
            return data_read_timer_expired(file_queue);
        });
    data_reader_job.set_bytes_read_callback([&stats](std::uint64_t bytes_read) {
        record_data_read_bytes(stats, bytes_read);
    });
    data_reader_job.set_file_read_callback([&stats]() {
        record_data_read_file(stats);
    });
    data_reader_job.set_file_failed_callback([&stats](const FileSpec&) {
        stats.files_failed.fetch_add(1, std::memory_order_relaxed);
    });

    BufferDiscarderJob discarder(BufferDiscarderConfig(1), reader_to_discard, registry);

    StatusRegistry status_registry;
    std::unique_ptr<StatusServer> status_server;
    if (!status_socket_path.empty()) {
        const std::string meta_reader_job_name = backend_job_name("MetaReader", data_config.source_root);
        const std::string data_reader_job_name = backend_job_name("DataReader", data_config.source_root);
        status_registry.register_job(meta_reader_job_name, [&stats, metadata_threads, meta_reader_job_name]() {
            const DataReadBenchmarkSnapshot stats_snapshot = snapshot_data_read_stats(stats);
            MonitorJobSnapshot snapshot;
            snapshot.name = meta_reader_job_name;
            snapshot.running = true;
            snapshot.worker_count = metadata_threads;
            snapshot.processed_count = stats_snapshot.files_found + stats_snapshot.folders_found;
            snapshot.byte_count = stats_snapshot.logical_size_bytes;
            snapshot.count_unit = "records";
            snapshot.detail = "files_found=" + std::to_string(stats_snapshot.files_found) +
                              " folders_found=" + std::to_string(stats_snapshot.folders_found);
            return snapshot;
        });
        status_registry.register_job(data_reader_job_name, [&data_reader_job, data_reader_job_name]() {
            const NfsDataBufferReaderStats stats_snapshot = data_reader_job.stats();
            MonitorJobSnapshot snapshot;
            snapshot.name = data_reader_job_name;
            snapshot.running = stats_snapshot.running;
            snapshot.worker_count = stats_snapshot.worker_count;
            snapshot.processed_count = stats_snapshot.buffers_read;
            snapshot.byte_count = stats_snapshot.bytes_read;
            snapshot.count_unit = "buffers";
            snapshot.has_runtime_metrics = true;
            snapshot.runtime_metrics = data_reader_job.runtime_metrics().snapshot();
            snapshot.detail = "files_read=" + std::to_string(stats_snapshot.files_read) +
                              " files_failed=" + std::to_string(stats_snapshot.files_failed);
            return snapshot;
        });
        status_registry.register_job("buffer_discarder", [&discarder]() {
            const BufferDiscarderStats stats_snapshot = discarder.stats();
            MonitorJobSnapshot snapshot;
            snapshot.name = "buffer_discarder";
            snapshot.running = stats_snapshot.running;
            snapshot.worker_count = stats_snapshot.worker_count;
            snapshot.processed_count = stats_snapshot.buffers_discarded;
            snapshot.byte_count = stats_snapshot.bytes_discarded;
            snapshot.count_unit = "buffers";
            snapshot.has_runtime_metrics = true;
            snapshot.runtime_metrics = discarder.runtime_metrics().snapshot();
            return snapshot;
        });
        status_registry.register_queue("folder_work_queue", [&folder_queue]() {
            return monitor_flat_folder_queue("folder_work_queue", folder_queue);
        });
        status_registry.register_queue("file_work_queue", [&file_queue]() {
            return monitor_data_file_queue("file_work_queue", file_queue);
        });
        status_registry.register_queue("reader_to_discard", [&reader_to_discard]() {
            return monitor_buf_queue("reader_to_discard", reader_to_discard);
        });
        status_server = std::make_unique<StatusServer>(status_socket_path, status_registry);
        status_server->start();
    }

    const auto benchmark_started_at = std::chrono::steady_clock::now();
    stats.started_at = benchmark_started_at;
    stats.last_print_at = benchmark_started_at;
    if (max_duration_seconds > 0.0) {
        const auto stop_at = benchmark_started_at +
                             std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                 std::chrono::duration<double>(max_duration_seconds));
        folder_queue.stop_at = stop_at;
        file_queue.stop_at = stop_at;
    }

    std::mutex stop_timer_mutex;
    std::condition_variable stop_timer_cv;
    bool cancel_stop_timer = false;
    std::thread stop_timer;
    if (folder_queue.stop_at.has_value()) {
        const auto stop_at = *folder_queue.stop_at;
        stop_timer = std::thread([&]() {
            std::unique_lock<std::mutex> lock(stop_timer_mutex);
            const bool cancelled = stop_timer_cv.wait_until(lock, stop_at, [&]() {
                return cancel_stop_timer;
            });
            if (!cancelled) {
                request_flat_folder_stop(folder_queue);
                request_data_file_stop(file_queue);
            }
        });
    }

    std::thread stats_printer(run_data_buffer_read_stats_printer,
                              std::ref(stats),
                              std::ref(file_queue),
                              std::cref(reader_to_discard));

    discarder.start();
    data_reader_job.start();

    std::vector<std::thread> metadata_workers;
    metadata_workers.reserve(metadata_threads);
    for (std::size_t index = 0; index < metadata_threads; ++index) {
        metadata_workers.emplace_back(scan_data_read_metadata_worker,
                                      meta_config.source_root,
                                      meta_config.recursive,
                                      std::max<std::size_t>(1, meta_config.async_directory_depth),
                                      meta_config.readdirplus_page_bytes,
                                      0,
                                      max_file_size_bytes,
                                      std::ref(folder_queue),
                                      std::ref(file_queue),
                                      std::ref(stats),
                                      nullptr,
                                      nullptr);
    }

    for (auto& worker : metadata_workers) {
        worker.join();
    }
    mark_data_file_input_done(file_queue);

    data_reader_job.wait();
    discarder.wait();

    stats.printer_done.store(true, std::memory_order_relaxed);
    stats.printer_cv.notify_all();
    if (stats_printer.joinable()) {
        stats_printer.join();
    }

    {
        std::lock_guard<std::mutex> lock(stop_timer_mutex);
        cancel_stop_timer = true;
    }
    stop_timer_cv.notify_all();
    if (stop_timer.joinable()) {
        stop_timer.join();
    }

    if (status_server) {
        status_server->stop();
    }

    if (folder_queue.error) {
        std::rethrow_exception(folder_queue.error);
    }
    if (file_queue.error) {
        std::rethrow_exception(file_queue.error);
    }

    DataReadBenchmarkSnapshot snapshot = snapshot_data_read_stats(stats);
    snapshot.data_buffer_slots = pool_slots;
    snapshot.data_queue_depth = queue_depth;
    return snapshot;
}

DataReadBenchmarkSnapshot run_parallel_data_write_scan(const NfsMetaReaderConfig& meta_config,
                                                       const NfsDataReaderConfig& data_config,
                                                       const TargetDataWriterConfig& writer_config,
                                                       std::uint64_t min_file_size_bytes,
                                                       std::uint64_t max_file_size_bytes,
                                                       std::size_t max_files_queued,
                                                       std::size_t data_buffer_slots,
                                                       std::size_t data_queue_depth,
                                                       double max_duration_seconds,
                                                       std::uint32_t stats_interval_seconds,
                                                       TargetWriterStats& writer_stats,
                                                       std::size_t& data_queue_capacity,
                                                       std::size_t& data_queue_high_watermark) {
    FlatMetadataWorkQueue folder_queue;
    folder_queue.folders.push_back(FileSpec{});
    DataReadFileQueue file_queue;
    file_queue.max_entries = std::max<std::size_t>(1, max_files_queued);

    DataReadBenchmarkStats stats;
    stats.print_interval_seconds = std::max<std::uint32_t>(1, stats_interval_seconds);
    stats.folders_found.store(1, std::memory_order_relaxed);
    reset_nfs_async_read_latency_metrics();

    TargetDataWriterConfig file_writer_config = writer_config;
    const std::size_t data_threads = std::max<std::size_t>(1, data_config.data_reader_worker_count);
    const bool create_target_directories = writer_config.ensure_parent_directories;
    const bool direct_reactor_submit = file_writer_config.direct_reactor_submit && is_nfs_url(file_writer_config.target_root) &&
                                       data_config.pack_small_files;
    const std::size_t writer_threads = direct_reactor_submit ? 0U : target_data_writer_effective_worker_count(file_writer_config);
    const std::size_t outstanding = std::max<std::size_t>(1, data_config.outstanding_requests);
    const std::size_t metadata_threads = std::max<std::size_t>(1, meta_config.worker_count);
    const std::size_t queue_depth_per_shard =
        std::max<std::size_t>(1, data_queue_depth == 0 ? std::max<std::size_t>(64, outstanding * 4U)
                                                       : data_queue_depth);
    const std::size_t total_queue_depth = direct_reactor_submit ? 0U : writer_threads * queue_depth_per_shard;
    const std::size_t minimum_pool_slots =
        direct_reactor_submit
            ? data_threads + std::max<std::size_t>(64U, data_threads / 4U) + 1U
            : data_threads * outstanding + total_queue_depth + std::max<std::size_t>(1U, writer_threads) + 1U;
    const std::size_t default_pool_slots =
        direct_reactor_submit ? minimum_pool_slots
                              : data_threads * outstanding * 3U + total_queue_depth + 1U;
    const std::size_t pool_slots =
        std::max<std::size_t>(minimum_pool_slots, data_buffer_slots == 0 ? default_pool_slots : data_buffer_slots);

    RawBufferPool data_pool = make_data_buffer_pool(pool_slots);
    std::unique_ptr<ShardedBufQueue> reader_to_writer;
    if (!direct_reactor_submit) {
        reader_to_writer = std::make_unique<ShardedBufQueue>(writer_threads, queue_depth_per_shard);
    }
    std::unique_ptr<RawBufferPool> target_metadata_pool;
    std::unique_ptr<BufQueue> target_metadata_queue;
    std::unique_ptr<TargetMetaWriterJob> target_meta_writer;
    if (create_target_directories) {
        const std::size_t metadata_slots = 4096U;
        target_metadata_pool = std::make_unique<RawBufferPool>(kMetadataBatchBufferPoolId,
                                                               metadata_slots,
                                                               sizeof(MetadataBatchBuffer),
                                                               alignof(MetadataBatchBuffer));
        target_metadata_queue = std::make_unique<BufQueue>(metadata_slots);
        TargetMetaWriterConfig meta_writer_config(std::max<std::size_t>(1U, writer_config.worker_count),
                                                  writer_config.target_root);
        meta_writer_config.preserve_metadata = false;
        meta_writer_config.async_window = writer_config.max_concurrent_file_transactions;
        target_meta_writer =
            std::make_unique<TargetMetaWriterJob>(meta_writer_config, *target_metadata_pool, *target_metadata_queue);
    }

    TargetWriterBackend::Options direct_options;
    direct_options.preserve_metadata = file_writer_config.preserve_metadata;
    direct_options.fsync_on_finish = file_writer_config.fsync_on_finish;
    direct_options.ensure_parent_directories = file_writer_config.ensure_parent_directories;
    direct_options.stable_small_file_writes = file_writer_config.stable_small_file_writes;
    direct_options.tcp_cork_small_file_writes = file_writer_config.tcp_cork_small_file_writes;
    direct_options.reactors_per_ip = std::max<std::size_t>(1U, file_writer_config.reactors_per_ip);
    direct_options.reactor_count = file_writer_config.reactor_count;
    direct_options.max_concurrent_file_transactions = file_writer_config.max_concurrent_file_transactions;
    std::unique_ptr<TargetWriterBackend> direct_backend;
    std::mutex direct_backend_mutex;
    DirectTargetWriterStats direct_writer_stats;
    if (direct_reactor_submit) {
        direct_backend = make_target_writer_backend(file_writer_config.target_root, 0, direct_options);
    }

    const auto direct_consume_buffer = [&](std::size_t, const BufferHandle& handle) {
        std::uint64_t payload_bytes = 0;
        std::size_t files_written = 0;
        try {
            const DataBuffer& buffer = data_buffer(data_pool, handle);
            if (hypersync::is_packed_small_file_buffer(buffer)) {
                std::vector<TargetWriterBackend::WriteChunk> files;
                files.reserve(hypersync::packed_small_file_count(buffer));
                const bool ok = hypersync::visit_packed_small_files(buffer, [&](hypersync::PackedSmallFileView view) {
                    FileSpec file;
                    file.rel_path = std::string(view.rel_path);
                    file.declared_size = view.file_size;
                    file.mtime = view.mtime;
                    file.mode = view.mode != 0U ? view.mode : 0644U;
                    file.uid = view.uid;
                    file.gid = view.gid;
                    TargetWriterBackend::WriteChunk chunk;
                    chunk.spec = std::move(file);
                    chunk.data = view.data;
                    chunk.offset = 0;
                    chunk.last_chunk = true;
                    payload_bytes += view.data.size();
                    files.push_back(std::move(chunk));
                });
                if (!ok) {
                    throw std::runtime_error("direct DataWriter-NFS received malformed packed-small-file buffer");
                }
                files_written = files.size();
                direct_backend->write_files(files);
            } else {
                const std::size_t data_len = static_cast<std::size_t>(buffer.trailer.data_len);
                if (data_len > buffer.bytes.size()) {
                    throw std::runtime_error("direct DataWriter-NFS received oversized data buffer");
                }
                TargetWriterBackend::WriteChunk chunk;
                chunk.spec.rel_path = std::string(buffer.trailer.rel_path.view());
                chunk.spec.declared_size = buffer.trailer.file_size;
                chunk.spec.mtime = buffer.trailer.mtime;
                chunk.spec.mode = buffer.trailer.mode != 0U ? buffer.trailer.mode : 0644U;
                chunk.spec.uid = buffer.trailer.uid;
                chunk.spec.gid = buffer.trailer.gid;
                chunk.data = std::string_view(reinterpret_cast<const char*>(buffer.bytes.data()), data_len);
                chunk.offset = buffer.trailer.data_offset;
                chunk.last_chunk = (buffer.trailer.flags & kFlagLastChunk) != 0U;
                payload_bytes = data_len;
                files_written = chunk.last_chunk ? 1U : 0U;
                std::vector<TargetWriterBackend::WriteChunk> chunks;
                chunks.push_back(std::move(chunk));
                std::lock_guard<std::mutex> lock(direct_backend_mutex);
                direct_backend->write_chunks(chunks);
            }
            direct_writer_stats.buffers_processed.fetch_add(1U, std::memory_order_relaxed);
            direct_writer_stats.files_written.fetch_add(files_written, std::memory_order_relaxed);
            direct_writer_stats.bytes_written.fetch_add(payload_bytes, std::memory_order_relaxed);
            data_pool.release(handle);
            return true;
        } catch (...) {
            direct_writer_stats.files_failed.fetch_add(1U, std::memory_order_relaxed);
            data_pool.release(handle);
            throw;
        }
    };

    const auto file_provider = [&file_queue]() {
            thread_local std::deque<FileSpec> worker_file_batch;
            if (worker_file_batch.empty()) {
                std::vector<FileSpec> next_batch = take_data_file_work_batch(file_queue, 128);
                for (auto& file : next_batch) {
                    worker_file_batch.push_back(std::move(file));
                }
            }
            if (worker_file_batch.empty()) {
                return std::optional<FileSpec> {};
            }
            FileSpec file = std::move(worker_file_batch.front());
            worker_file_batch.pop_front();
            return std::optional<FileSpec> {std::move(file)};
    };
    const auto stop_predicate = [&file_queue]() {
            return data_read_timer_expired(file_queue);
    };

    std::unique_ptr<NfsDataBufferReaderJob> data_reader_job;
    if (direct_reactor_submit) {
        data_reader_job = std::make_unique<NfsDataBufferReaderJob>(
            data_config,
            data_pool,
            NfsDataBufferReaderJob::BufferConsumer(direct_consume_buffer),
            file_provider,
            stop_predicate);
    } else {
        data_reader_job = std::make_unique<NfsDataBufferReaderJob>(
            data_config,
            data_pool,
            *reader_to_writer,
            file_provider,
            stop_predicate);
    }
    data_reader_job->set_bytes_read_callback([&stats](std::uint64_t bytes_read) {
        record_data_read_bytes(stats, bytes_read);
    });
    data_reader_job->set_file_read_callback([&stats]() {
        record_data_read_file(stats);
    });
    data_reader_job->set_file_failed_callback([&stats](const FileSpec&) {
        stats.files_failed.fetch_add(1, std::memory_order_relaxed);
    });

    std::unique_ptr<TargetDataWriterJob> writer_job;
    if (!direct_reactor_submit) {
        writer_job = std::make_unique<TargetDataWriterJob>(file_writer_config, data_pool, *reader_to_writer);
    }

    const auto benchmark_started_at = std::chrono::steady_clock::now();
    stats.started_at = benchmark_started_at;
    stats.last_print_at = benchmark_started_at;
    if (max_duration_seconds > 0.0) {
        const auto stop_at = benchmark_started_at +
                             std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                 std::chrono::duration<double>(max_duration_seconds));
        folder_queue.stop_at = stop_at;
        file_queue.stop_at = stop_at;
    }

    std::mutex stop_timer_mutex;
    std::condition_variable stop_timer_cv;
    bool cancel_stop_timer = false;
    std::thread stop_timer;
    if (folder_queue.stop_at.has_value()) {
        const auto stop_at = *folder_queue.stop_at;
        stop_timer = std::thread([&]() {
            std::unique_lock<std::mutex> lock(stop_timer_mutex);
            const bool cancelled = stop_timer_cv.wait_until(lock, stop_at, [&]() {
                return cancel_stop_timer;
            });
            if (!cancelled) {
                request_flat_folder_stop(folder_queue);
                request_data_file_stop(file_queue);
            }
        });
    }

    std::thread stats_printer;
    if (direct_reactor_submit) {
        stats_printer = std::thread(run_direct_data_buffer_write_stats_printer,
                                    std::ref(stats),
                                    std::ref(file_queue),
                                    std::cref(direct_writer_stats));
    } else {
        stats_printer = std::thread(run_data_buffer_write_stats_printer,
                                    std::ref(stats),
                                    std::ref(file_queue),
                                    std::cref(*reader_to_writer),
                                    std::cref(*writer_job));
    }

    if (writer_job) {
        writer_job->start();
    }
    if (target_meta_writer) {
        target_meta_writer->start();
    }

    std::vector<std::thread> metadata_workers;
    metadata_workers.reserve(metadata_threads);
    for (std::size_t index = 0; index < metadata_threads; ++index) {
        metadata_workers.emplace_back(scan_data_read_metadata_worker,
                                      meta_config.source_root,
                                      meta_config.recursive,
                                      std::max<std::size_t>(1, meta_config.async_directory_depth),
                                      meta_config.readdirplus_page_bytes,
                                      min_file_size_bytes,
                                      max_file_size_bytes,
                                      std::ref(folder_queue),
                                      std::ref(file_queue),
                                      std::ref(stats),
                                      target_metadata_pool.get(),
                                      target_metadata_queue.get());
    }
    if (target_meta_writer) {
        const auto warmup_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (std::chrono::steady_clock::now() < warmup_deadline &&
               !data_read_timer_expired(file_queue)) {
            const TargetWriterStats meta_stats = target_meta_writer->stats();
            if (meta_stats.folders_written >= 1024U) {
                break;
            }
            if (target_metadata_queue != nullptr &&
                target_metadata_queue->empty() &&
                queued_data_read_files(file_queue) >= 1024U) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    data_reader_job->start();

    std::exception_ptr pipeline_error;
    for (auto& worker : metadata_workers) {
        worker.join();
    }
    mark_data_file_input_done(file_queue);
    if (target_metadata_queue) {
        target_metadata_queue->close();
    }

    try {
        data_reader_job->wait();
        if (writer_job) {
            writer_job->wait();
        }
        if (target_meta_writer) {
            target_meta_writer->wait();
        }
    } catch (...) {
        pipeline_error = std::current_exception();
        try {
            data_reader_job->stop();
        } catch (...) {}
        if (writer_job) {
            try {
                writer_job->stop();
            } catch (...) {}
        }
        if (target_meta_writer) {
            try {
                target_meta_writer->stop();
            } catch (...) {}
        }
    }

    stats.printer_done.store(true, std::memory_order_relaxed);
    stats.printer_cv.notify_all();
    if (stats_printer.joinable()) {
        stats_printer.join();
    }

    {
        std::lock_guard<std::mutex> lock(stop_timer_mutex);
        cancel_stop_timer = true;
    }
    stop_timer_cv.notify_all();
    if (stop_timer.joinable()) {
        stop_timer.join();
    }

    if (folder_queue.error) {
        std::rethrow_exception(folder_queue.error);
    }
    if (file_queue.error) {
        std::rethrow_exception(file_queue.error);
    }
    if (pipeline_error) {
        std::rethrow_exception(pipeline_error);
    }

    writer_stats = direct_reactor_submit ? direct_writer_stats.snapshot() : writer_job->stats();
    if (target_meta_writer) {
        const TargetWriterStats meta_writer_stats = target_meta_writer->stats();
        writer_stats.folders_written += meta_writer_stats.folders_written;
    }
    data_queue_capacity = direct_reactor_submit ? 0U : reader_to_writer->capacity();
    data_queue_high_watermark = direct_reactor_submit ? 0U : reader_to_writer->high_watermark();

    DataReadBenchmarkSnapshot snapshot = snapshot_data_read_stats(stats);
    snapshot.data_buffer_slots = pool_slots;
    snapshot.data_queue_depth = total_queue_depth;
    return snapshot;
}

DataReadBenchmarkSnapshot run_parallel_folder_ready_discard_scan(const NfsMetaReaderConfig& meta_config,
                                                                 const TargetDataWriterConfig& writer_config,
                                                                 std::size_t file_discard_threads,
                                                                 std::uint64_t min_file_size_bytes,
                                                                 std::uint64_t max_file_size_bytes,
                                                                 std::size_t max_files_queued,
                                                                 double max_duration_seconds,
                                                                 std::uint32_t stats_interval_seconds,
                                                                 TargetWriterStats& writer_stats,
                                                                 std::size_t& queue_capacity,
                                                                 std::size_t& queue_high_watermark) {
    FlatMetadataWorkQueue scan_folder_queue;
    scan_folder_queue.folders.push_back(FileSpec{});
    FolderReadyBatchQueue folder_batch_queue;
    folder_batch_queue.max_entries = 4096U;
    DataReadFileQueue ready_file_queue;
    ready_file_queue.max_entries = std::max<std::size_t>(1U, max_files_queued);

    DataReadBenchmarkStats stats;
    stats.print_interval_seconds = std::max<std::uint32_t>(1U, stats_interval_seconds);
    stats.folders_found.store(1U, std::memory_order_relaxed);
    DirectTargetWriterStats discard_stats;
    std::atomic<std::uint64_t> folders_created {0};

    const auto started = std::chrono::steady_clock::now();
    stats.started_at = started;
    stats.last_print_at = started;
    if (max_duration_seconds > 0.0) {
        const auto stop_at = started +
                             std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                 std::chrono::duration<double>(max_duration_seconds));
        scan_folder_queue.stop_at = stop_at;
        folder_batch_queue.stop_at = stop_at;
        ready_file_queue.stop_at = stop_at;
    }

    std::mutex stop_timer_mutex;
    std::condition_variable stop_timer_cv;
    bool cancel_stop_timer = false;
    std::thread stop_timer;
    if (scan_folder_queue.stop_at.has_value()) {
        const auto stop_at = *scan_folder_queue.stop_at;
        stop_timer = std::thread([&]() {
            std::unique_lock<std::mutex> lock(stop_timer_mutex);
            const bool cancelled = stop_timer_cv.wait_until(lock, stop_at, [&]() {
                return cancel_stop_timer;
            });
            if (!cancelled) {
                request_flat_folder_stop(scan_folder_queue);
                request_folder_ready_stop(folder_batch_queue);
                request_data_file_stop(ready_file_queue);
            }
        });
    }

    std::thread stats_printer(run_folder_ready_discard_stats_printer,
                              std::ref(stats),
                              std::ref(folder_batch_queue),
                              std::ref(ready_file_queue),
                              std::cref(folders_created),
                              std::cref(discard_stats));

    std::exception_ptr pipeline_error;
    const std::size_t folder_create_threads = target_data_writer_effective_worker_count(writer_config);
    std::vector<std::thread> folder_workers;
    folder_workers.reserve(folder_create_threads);
    for (std::size_t index = 0; index < folder_create_threads; ++index) {
        folder_workers.emplace_back([&, index]() {
            TargetWriterBackend::Options options;
            options.preserve_metadata = false;
            options.fsync_on_finish = false;
            options.ensure_parent_directories = false;
            options.max_concurrent_file_transactions =
                std::max<std::size_t>(1U, writer_config.max_concurrent_file_transactions);
            auto backend = make_target_writer_backend(writer_config.target_root, index, options);
            try {
                while (true) {
                    std::optional<FolderReadyFileBatch> batch = take_folder_ready_batch(folder_batch_queue);
                    if (!batch.has_value()) {
                        break;
                    }
                    if (!batch->directories.empty()) {
                        backend->ensure_directories(batch->directories);
                        folders_created.fetch_add(batch->directories.size(), std::memory_order_relaxed);
                    }
                    if (!batch->files.empty() &&
                        !enqueue_data_read_files(ready_file_queue, std::move(batch->files))) {
                        break;
                    }
                }
            } catch (...) {
                const std::exception_ptr error = std::current_exception();
                fail_folder_ready_work(folder_batch_queue, error);
                fail_data_file_work(ready_file_queue, error);
            }
        });
    }

    const std::size_t sink_threads = std::max<std::size_t>(1U, file_discard_threads);
    std::vector<std::thread> discard_workers;
    discard_workers.reserve(sink_threads);
    for (std::size_t index = 0; index < sink_threads; ++index) {
        (void) index;
        discard_workers.emplace_back([&]() {
            try {
                while (true) {
                    std::vector<FileSpec> files = take_data_file_work_batch(ready_file_queue, 256U);
                    if (files.empty()) {
                        break;
                    }
                    std::uint64_t bytes = 0;
                    for (const FileSpec& file : files) {
                        bytes += file.declared_size != 0U ? file.declared_size : file.content.size();
                    }
                    stats.files_read.fetch_add(files.size(), std::memory_order_relaxed);
                    stats.bytes_read.fetch_add(bytes, std::memory_order_relaxed);
                    discard_stats.files_written.fetch_add(files.size(), std::memory_order_relaxed);
                    discard_stats.bytes_written.fetch_add(bytes, std::memory_order_relaxed);
                }
            } catch (...) {
                const std::exception_ptr error = std::current_exception();
                fail_data_file_work(ready_file_queue, error);
            }
        });
    }

    const std::size_t metadata_threads = std::max<std::size_t>(1U, meta_config.worker_count);
    std::vector<std::thread> metadata_workers;
    metadata_workers.reserve(metadata_threads);
    for (std::size_t index = 0; index < metadata_threads; ++index) {
        metadata_workers.emplace_back(scan_folder_ready_metadata_worker,
                                      meta_config.source_root,
                                      meta_config.recursive,
                                      std::max<std::size_t>(1U, meta_config.async_directory_depth),
                                      meta_config.readdirplus_page_bytes,
                                      min_file_size_bytes,
                                      max_file_size_bytes,
                                      std::ref(scan_folder_queue),
                                      std::ref(folder_batch_queue),
                                      std::ref(ready_file_queue),
                                      std::ref(stats));
    }

    for (auto& worker : metadata_workers) {
        worker.join();
    }
    mark_folder_ready_input_done(folder_batch_queue);
    for (auto& worker : folder_workers) {
        worker.join();
    }
    mark_data_file_input_done(ready_file_queue);
    for (auto& worker : discard_workers) {
        worker.join();
    }

    stats.printer_done.store(true, std::memory_order_relaxed);
    stats.printer_cv.notify_all();
    if (stats_printer.joinable()) {
        stats_printer.join();
    }

    {
        std::lock_guard<std::mutex> lock(stop_timer_mutex);
        cancel_stop_timer = true;
    }
    stop_timer_cv.notify_all();
    if (stop_timer.joinable()) {
        stop_timer.join();
    }

    if (scan_folder_queue.error) {
        std::rethrow_exception(scan_folder_queue.error);
    }
    if (folder_batch_queue.error) {
        std::rethrow_exception(folder_batch_queue.error);
    }
    if (ready_file_queue.error) {
        std::rethrow_exception(ready_file_queue.error);
    }
    if (pipeline_error) {
        std::rethrow_exception(pipeline_error);
    }

    writer_stats = discard_stats.snapshot();
    writer_stats.worker_count = sink_threads;
    writer_stats.folders_written = folders_created.load(std::memory_order_relaxed);
    queue_capacity = folder_batch_queue.max_entries;
    queue_high_watermark = folder_batch_queue.high_watermark;

    DataReadBenchmarkSnapshot snapshot = snapshot_data_read_stats(stats);
    snapshot.folders_written = static_cast<std::size_t>(writer_stats.folders_written);
    snapshot.folders_per_second = snapshot.elapsed_seconds > 0.0
                                      ? static_cast<double>(snapshot.folders_written) / snapshot.elapsed_seconds
                                      : 0.0;
    snapshot.data_queue_depth = ready_file_queue.max_entries;
    return snapshot;
}

DataReadBenchmarkSnapshot run_parallel_folder_ready_write_scan(const NfsMetaReaderConfig& meta_config,
                                                               const NfsDataReaderConfig& data_config,
                                                               const TargetDataWriterConfig& writer_config,
                                                               std::uint64_t min_file_size_bytes,
                                                               std::uint64_t max_file_size_bytes,
                                                               std::size_t max_files_queued,
                                                               std::size_t data_buffer_slots,
                                                               std::size_t data_queue_depth,
                                                               double max_duration_seconds,
                                                               std::uint32_t stats_interval_seconds,
                                                               TargetWriterStats& writer_stats,
                                                               std::size_t& queue_capacity,
                                                               std::size_t& queue_high_watermark) {
    FlatMetadataWorkQueue scan_folder_queue;
    scan_folder_queue.folders.push_back(FileSpec{});
    FolderReadyBatchQueue folder_batch_queue;
    folder_batch_queue.max_entries = 4096U;
    DataReadFileQueue ready_file_queue;
    ready_file_queue.max_entries = std::max<std::size_t>(1U, max_files_queued);

    DataReadBenchmarkStats stats;
    stats.print_interval_seconds = std::max<std::uint32_t>(1U, stats_interval_seconds);
    stats.folders_found.store(1U, std::memory_order_relaxed);
    reset_nfs_async_read_latency_metrics();
    std::atomic<std::uint64_t> folders_created {0};

    TargetDataWriterConfig file_writer_config = writer_config;
    file_writer_config.ensure_parent_directories = false;
    const std::size_t data_threads = std::max<std::size_t>(1U, data_config.data_reader_worker_count);
    const bool direct_reactor_submit = file_writer_config.direct_reactor_submit &&
                                       is_nfs_url(file_writer_config.target_root) &&
                                       data_config.pack_small_files;
    const std::size_t writer_threads = direct_reactor_submit
                                           ? 0U
                                           : target_data_writer_effective_worker_count(file_writer_config);
    const std::size_t outstanding = std::max<std::size_t>(1U, data_config.outstanding_requests);
    const std::size_t queue_depth_per_shard =
        std::max<std::size_t>(1U, data_queue_depth == 0U ? std::max<std::size_t>(64U, outstanding * 4U)
                                                         : data_queue_depth);
    const std::size_t total_queue_depth = direct_reactor_submit ? 0U : writer_threads * queue_depth_per_shard;
    const std::size_t minimum_pool_slots =
        direct_reactor_submit
            ? data_threads + std::max<std::size_t>(64U, data_threads / 4U) + 1U
            : data_threads * outstanding + total_queue_depth + std::max<std::size_t>(1U, writer_threads) + 1U;
    const std::size_t default_pool_slots =
        direct_reactor_submit ? minimum_pool_slots
                              : data_threads * outstanding * 3U + total_queue_depth + 1U;
    const std::size_t pool_slots =
        std::max<std::size_t>(minimum_pool_slots, data_buffer_slots == 0U ? default_pool_slots : data_buffer_slots);

    RawBufferPool data_pool = make_data_buffer_pool(pool_slots);
    std::unique_ptr<ShardedBufQueue> reader_to_writer;
    if (!direct_reactor_submit) {
        reader_to_writer = std::make_unique<ShardedBufQueue>(writer_threads, queue_depth_per_shard);
    }

    TargetWriterBackend::Options direct_options;
    direct_options.preserve_metadata = file_writer_config.preserve_metadata;
    direct_options.fsync_on_finish = file_writer_config.fsync_on_finish;
    direct_options.ensure_parent_directories = false;
    direct_options.stable_small_file_writes = file_writer_config.stable_small_file_writes;
    direct_options.tcp_cork_small_file_writes = file_writer_config.tcp_cork_small_file_writes;
    direct_options.reactors_per_ip = std::max<std::size_t>(1U, file_writer_config.reactors_per_ip);
    direct_options.reactor_count = file_writer_config.reactor_count;
    direct_options.max_concurrent_file_transactions = file_writer_config.max_concurrent_file_transactions;
    std::unique_ptr<TargetWriterBackend> direct_backend;
    std::mutex direct_backend_mutex;
    DirectTargetWriterStats direct_writer_stats;
    if (direct_reactor_submit) {
        direct_backend = make_target_writer_backend(file_writer_config.target_root, 0U, direct_options);
    }

    const auto direct_consume_buffer = [&](std::size_t, const BufferHandle& handle) {
        std::uint64_t payload_bytes = 0;
        std::size_t files_written = 0;
        try {
            const DataBuffer& buffer = data_buffer(data_pool, handle);
            if (hypersync::is_packed_small_file_buffer(buffer)) {
                std::vector<TargetWriterBackend::WriteChunk> files;
                files.reserve(hypersync::packed_small_file_count(buffer));
                const bool ok = hypersync::visit_packed_small_files(buffer, [&](hypersync::PackedSmallFileView view) {
                    FileSpec file;
                    file.rel_path = std::string(view.rel_path);
                    file.declared_size = view.file_size;
                    file.mtime = view.mtime;
                    file.mode = view.mode != 0U ? view.mode : 0644U;
                    file.uid = view.uid;
                    file.gid = view.gid;
                    TargetWriterBackend::WriteChunk chunk;
                    chunk.spec = std::move(file);
                    chunk.data = view.data;
                    chunk.offset = 0;
                    chunk.last_chunk = true;
                    payload_bytes += view.data.size();
                    files.push_back(std::move(chunk));
                });
                if (!ok) {
                    throw std::runtime_error("direct DataWriter-NFS received malformed packed-small-file buffer");
                }
                files_written = files.size();
                direct_backend->write_files(files);
            } else {
                const std::size_t data_len = static_cast<std::size_t>(buffer.trailer.data_len);
                if (data_len > buffer.bytes.size()) {
                    throw std::runtime_error("direct DataWriter-NFS received oversized data buffer");
                }
                TargetWriterBackend::WriteChunk chunk;
                chunk.spec.rel_path = std::string(buffer.trailer.rel_path.view());
                chunk.spec.declared_size = buffer.trailer.file_size;
                chunk.spec.mtime = buffer.trailer.mtime;
                chunk.spec.mode = buffer.trailer.mode != 0U ? buffer.trailer.mode : 0644U;
                chunk.spec.uid = buffer.trailer.uid;
                chunk.spec.gid = buffer.trailer.gid;
                chunk.data = std::string_view(reinterpret_cast<const char*>(buffer.bytes.data()), data_len);
                chunk.offset = buffer.trailer.data_offset;
                chunk.last_chunk = (buffer.trailer.flags & kFlagLastChunk) != 0U;
                payload_bytes = data_len;
                files_written = chunk.last_chunk ? 1U : 0U;
                std::vector<TargetWriterBackend::WriteChunk> chunks;
                chunks.push_back(std::move(chunk));
                std::lock_guard<std::mutex> lock(direct_backend_mutex);
                direct_backend->write_chunks(chunks);
            }
            direct_writer_stats.buffers_processed.fetch_add(1U, std::memory_order_relaxed);
            direct_writer_stats.files_written.fetch_add(files_written, std::memory_order_relaxed);
            direct_writer_stats.bytes_written.fetch_add(payload_bytes, std::memory_order_relaxed);
            data_pool.release(handle);
            return true;
        } catch (...) {
            direct_writer_stats.files_failed.fetch_add(1U, std::memory_order_relaxed);
            data_pool.release(handle);
            throw;
        }
    };

    const auto file_provider = [&ready_file_queue]() {
        thread_local std::deque<FileSpec> worker_file_batch;
        if (worker_file_batch.empty()) {
            std::vector<FileSpec> next_batch = take_data_file_work_batch(ready_file_queue, 128U);
            for (auto& file : next_batch) {
                worker_file_batch.push_back(std::move(file));
            }
        }
        if (worker_file_batch.empty()) {
            return std::optional<FileSpec> {};
        }
        FileSpec file = std::move(worker_file_batch.front());
        worker_file_batch.pop_front();
        return std::optional<FileSpec> {std::move(file)};
    };
    const auto stop_predicate = [&ready_file_queue]() {
        return data_read_timer_expired(ready_file_queue);
    };

    std::unique_ptr<NfsDataBufferReaderJob> data_reader_job;
    if (direct_reactor_submit) {
        data_reader_job = std::make_unique<NfsDataBufferReaderJob>(
            data_config,
            data_pool,
            NfsDataBufferReaderJob::BufferConsumer(direct_consume_buffer),
            file_provider,
            stop_predicate);
    } else {
        data_reader_job = std::make_unique<NfsDataBufferReaderJob>(
            data_config,
            data_pool,
            *reader_to_writer,
            file_provider,
            stop_predicate);
    }
    data_reader_job->set_bytes_read_callback([&stats](std::uint64_t bytes_read) {
        record_data_read_bytes(stats, bytes_read);
    });
    data_reader_job->set_file_read_callback([&stats]() {
        record_data_read_file(stats);
    });
    data_reader_job->set_file_failed_callback([&stats](const FileSpec&) {
        stats.files_failed.fetch_add(1U, std::memory_order_relaxed);
    });

    std::unique_ptr<TargetDataWriterJob> writer_job;
    if (!direct_reactor_submit) {
        writer_job = std::make_unique<TargetDataWriterJob>(file_writer_config, data_pool, *reader_to_writer);
    }

    const auto started = std::chrono::steady_clock::now();
    stats.started_at = started;
    stats.last_print_at = started;
    if (max_duration_seconds > 0.0) {
        const auto stop_at = started +
                             std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                 std::chrono::duration<double>(max_duration_seconds));
        scan_folder_queue.stop_at = stop_at;
        folder_batch_queue.stop_at = stop_at;
        ready_file_queue.stop_at = stop_at;
    }

    std::mutex stop_timer_mutex;
    std::condition_variable stop_timer_cv;
    bool cancel_stop_timer = false;
    std::thread stop_timer;
    if (scan_folder_queue.stop_at.has_value()) {
        const auto stop_at = *scan_folder_queue.stop_at;
        stop_timer = std::thread([&]() {
            std::unique_lock<std::mutex> lock(stop_timer_mutex);
            const bool cancelled = stop_timer_cv.wait_until(lock, stop_at, [&]() {
                return cancel_stop_timer;
            });
            if (!cancelled) {
                request_flat_folder_stop(scan_folder_queue);
                request_folder_ready_stop(folder_batch_queue);
                request_data_file_stop(ready_file_queue);
            }
        });
    }

    std::thread stats_printer;
    if (direct_reactor_submit) {
        stats_printer = std::thread(run_direct_data_buffer_write_stats_printer,
                                    std::ref(stats),
                                    std::ref(ready_file_queue),
                                    std::cref(direct_writer_stats));
    } else {
        stats_printer = std::thread(run_data_buffer_write_stats_printer,
                                    std::ref(stats),
                                    std::ref(ready_file_queue),
                                    std::cref(*reader_to_writer),
                                    std::cref(*writer_job));
    }

    std::exception_ptr pipeline_error;
    const std::size_t folder_create_threads = target_data_writer_effective_worker_count(writer_config);
    std::vector<std::thread> folder_workers;
    folder_workers.reserve(folder_create_threads);
    for (std::size_t index = 0; index < folder_create_threads; ++index) {
        folder_workers.emplace_back([&, index]() {
            TargetWriterBackend::Options options;
            options.preserve_metadata = false;
            options.fsync_on_finish = false;
            options.ensure_parent_directories = false;
            options.max_concurrent_file_transactions =
                std::max<std::size_t>(1U, writer_config.max_concurrent_file_transactions);
            auto backend = make_target_writer_backend(writer_config.target_root, index, options);
            try {
                while (true) {
                    std::optional<FolderReadyFileBatch> batch = take_folder_ready_batch(folder_batch_queue);
                    if (!batch.has_value()) {
                        break;
                    }
                    if (!batch->directories.empty()) {
                        backend->ensure_directories(batch->directories);
                        folders_created.fetch_add(batch->directories.size(), std::memory_order_relaxed);
                    }
                    if (!batch->files.empty() &&
                        !enqueue_data_read_files(ready_file_queue, std::move(batch->files))) {
                        break;
                    }
                }
            } catch (...) {
                const std::exception_ptr error = std::current_exception();
                fail_folder_ready_work(folder_batch_queue, error);
                fail_data_file_work(ready_file_queue, error);
            }
        });
    }

    if (writer_job) {
        writer_job->start();
    }
    data_reader_job->start();

    const std::size_t metadata_threads = std::max<std::size_t>(1U, meta_config.worker_count);
    std::vector<std::thread> metadata_workers;
    metadata_workers.reserve(metadata_threads);
    for (std::size_t index = 0; index < metadata_threads; ++index) {
        metadata_workers.emplace_back(scan_folder_ready_metadata_worker,
                                      meta_config.source_root,
                                      meta_config.recursive,
                                      std::max<std::size_t>(1U, meta_config.async_directory_depth),
                                      meta_config.readdirplus_page_bytes,
                                      min_file_size_bytes,
                                      max_file_size_bytes,
                                      std::ref(scan_folder_queue),
                                      std::ref(folder_batch_queue),
                                      std::ref(ready_file_queue),
                                      std::ref(stats));
    }

    for (auto& worker : metadata_workers) {
        worker.join();
    }
    mark_folder_ready_input_done(folder_batch_queue);
    for (auto& worker : folder_workers) {
        worker.join();
    }
    mark_data_file_input_done(ready_file_queue);

    try {
        data_reader_job->wait();
        if (writer_job) {
            writer_job->wait();
        }
    } catch (...) {
        pipeline_error = std::current_exception();
        try {
            data_reader_job->stop();
        } catch (...) {}
        if (writer_job) {
            try {
                writer_job->stop();
            } catch (...) {}
        }
    }

    stats.printer_done.store(true, std::memory_order_relaxed);
    stats.printer_cv.notify_all();
    if (stats_printer.joinable()) {
        stats_printer.join();
    }

    {
        std::lock_guard<std::mutex> lock(stop_timer_mutex);
        cancel_stop_timer = true;
    }
    stop_timer_cv.notify_all();
    if (stop_timer.joinable()) {
        stop_timer.join();
    }

    if (scan_folder_queue.error) {
        std::rethrow_exception(scan_folder_queue.error);
    }
    if (folder_batch_queue.error) {
        std::rethrow_exception(folder_batch_queue.error);
    }
    if (ready_file_queue.error) {
        std::rethrow_exception(ready_file_queue.error);
    }
    if (pipeline_error) {
        std::rethrow_exception(pipeline_error);
    }

    writer_stats = direct_reactor_submit ? direct_writer_stats.snapshot() : writer_job->stats();
    writer_stats.folders_written = folders_created.load(std::memory_order_relaxed);
    queue_capacity = folder_batch_queue.max_entries;
    queue_high_watermark = folder_batch_queue.high_watermark;

    DataReadBenchmarkSnapshot snapshot = snapshot_data_read_stats(stats);
    snapshot.folders_written = static_cast<std::size_t>(writer_stats.folders_written);
    snapshot.folders_per_second = snapshot.elapsed_seconds > 0.0
                                      ? static_cast<double>(snapshot.folders_written) / snapshot.elapsed_seconds
                                      : 0.0;
    snapshot.data_buffer_slots = pool_slots;
    snapshot.data_queue_depth = direct_reactor_submit ? ready_file_queue.max_entries : total_queue_depth;
    return snapshot;
}

DataReadBenchmarkSnapshot run_parallel_folder_ready_mixed_write_scan(const NfsMetaReaderConfig& meta_config,
                                                                     const NfsDataReaderConfig& data_config,
                                                                     const TargetDataWriterConfig& writer_config,
                                                                     std::uint64_t small_file_threshold,
                                                                     std::size_t max_files_queued,
                                                                     std::size_t data_buffer_slots,
                                                                     std::size_t data_queue_depth,
                                                                     double max_duration_seconds,
                                                                     std::uint32_t stats_interval_seconds,
                                                                     TargetWriterStats& writer_stats,
                                                                     std::size_t& queue_capacity,
                                                                     std::size_t& queue_high_watermark,
                                                                     const std::string& small_file_target_ips,
                                                                     const std::string& large_file_target_ips) {
    FlatMetadataWorkQueue scan_folder_queue;
    scan_folder_queue.folders.push_back(FileSpec{});
    FolderReadyBatchQueue folder_batch_queue;
    folder_batch_queue.max_entries = 4096U;

    const std::uint64_t true_small_threshold = 128U * 1024U;
    const std::uint64_t medium_threshold =
        small_file_threshold == 0U ? (1024U * 1024U - 1U)
                                   : std::max<std::uint64_t>(true_small_threshold, small_file_threshold);
    SplitDataReadFileQueues ready_queues;
    ready_queues.small_file_threshold = true_small_threshold;
    ready_queues.small.max_entries = std::max<std::size_t>(1U, max_files_queued);
    ready_queues.large.max_entries = std::max<std::size_t>(1U, max_files_queued);
    DataReadFileQueue medium_ready_queue;
    medium_ready_queue.max_entries = std::max<std::size_t>(1U, max_files_queued);
    ReadyFileSpillway medium_spillway;
    ReadyFileSpillway large_spillway;

    DataReadBenchmarkStats stats;
    stats.print_interval_seconds = std::max<std::uint32_t>(1U, stats_interval_seconds);
    stats.folders_found.store(1U, std::memory_order_relaxed);
    reset_nfs_async_read_latency_metrics();
    std::atomic<std::uint64_t> folders_created {0};

    TargetDataWriterConfig small_writer_config = writer_config;
    small_writer_config.target_root =
        nfs_url_with_server_expression(writer_config.target_root, small_file_target_ips);
    small_writer_config.ensure_parent_directories = false;
    small_writer_config.direct_reactor_submit = true;
    small_writer_config.direct_reactor_writes = true;
    small_writer_config.reactor_count = small_writer_config.reactor_count == 0U ? 64U : small_writer_config.reactor_count;
    small_writer_config.max_concurrent_file_transactions =
        small_writer_config.max_concurrent_file_transactions == 0U ? 64U
                                                                   : small_writer_config.max_concurrent_file_transactions;

    TargetDataWriterConfig large_writer_config = writer_config;
    large_writer_config.target_root =
        nfs_url_with_server_expression(writer_config.target_root, large_file_target_ips);
    large_writer_config.ensure_parent_directories = false;
    large_writer_config.direct_reactor_submit = false;
    large_writer_config.direct_reactor_writes = false;
    large_writer_config.worker_count = 112U;
    large_writer_config.async_window = 2U;

    TargetDataWriterConfig medium_writer_config = writer_config;
    medium_writer_config.target_root =
        nfs_url_with_server_expression(writer_config.target_root, large_file_target_ips);
    medium_writer_config.ensure_parent_directories = false;
    medium_writer_config.direct_reactor_submit = false;
    medium_writer_config.direct_reactor_writes = false;
    medium_writer_config.worker_count = 64U;
    medium_writer_config.async_window = 2U;

    NfsDataReaderConfig small_data_config = data_config;
    small_data_config.pack_small_files = true;
    small_data_config.small_file_threshold = ready_queues.small_file_threshold;
    NfsDataReaderConfig large_data_config = data_config;
    large_data_config.pack_small_files = false;
    large_data_config.data_reader_worker_count = 112U;
    large_data_config.outstanding_requests = 2U;
    large_data_config.small_file_async_window = 2U;
    large_data_config.small_file_threshold = 0U;
    NfsDataReaderConfig medium_data_config = data_config;
    medium_data_config.pack_small_files = false;
    medium_data_config.data_reader_worker_count = 64U;
    medium_data_config.outstanding_requests = 2U;
    medium_data_config.small_file_async_window = 2U;
    medium_data_config.small_file_threshold = 0U;

    const std::size_t small_data_threads = std::max<std::size_t>(1U, small_data_config.data_reader_worker_count);
    const std::size_t medium_data_threads = std::max<std::size_t>(1U, medium_data_config.data_reader_worker_count);
    const std::size_t large_data_threads = std::max<std::size_t>(1U, large_data_config.data_reader_worker_count);
    const std::size_t medium_writer_threads = target_data_writer_effective_worker_count(medium_writer_config);
    const std::size_t large_writer_threads = target_data_writer_effective_worker_count(large_writer_config);
    const std::size_t large_queue_depth_per_shard =
        std::max<std::size_t>(1U, data_queue_depth == 0U ? 512U : data_queue_depth);
    const std::size_t medium_queue_depth_per_shard = large_queue_depth_per_shard;
    const std::size_t medium_total_queue_depth = medium_writer_threads * medium_queue_depth_per_shard;
    const std::size_t large_total_queue_depth = large_writer_threads * large_queue_depth_per_shard;
    const std::size_t minimum_pool_slots =
        small_data_threads + std::max<std::size_t>(64U, small_data_threads / 4U) + 1U +
        medium_data_threads * std::max<std::size_t>(1U, medium_data_config.outstanding_requests) +
        medium_total_queue_depth + medium_writer_threads +
        large_data_threads * std::max<std::size_t>(1U, large_data_config.outstanding_requests) +
        large_total_queue_depth + large_writer_threads;
    const std::size_t default_pool_slots = minimum_pool_slots + (medium_data_threads + large_data_threads) * 4U;
    const std::size_t pool_slots =
        std::max<std::size_t>(minimum_pool_slots, data_buffer_slots == 0U ? default_pool_slots : data_buffer_slots);

    RawBufferPool data_pool = make_data_buffer_pool(pool_slots);
    ShardedBufQueue medium_reader_to_writer(medium_writer_threads, medium_queue_depth_per_shard);
    ShardedBufQueue large_reader_to_writer(large_writer_threads, large_queue_depth_per_shard);

    TargetWriterBackend::Options small_direct_options;
    small_direct_options.preserve_metadata = small_writer_config.preserve_metadata;
    small_direct_options.fsync_on_finish = small_writer_config.fsync_on_finish;
    small_direct_options.ensure_parent_directories = false;
    small_direct_options.stable_small_file_writes = small_writer_config.stable_small_file_writes;
    small_direct_options.tcp_cork_small_file_writes = small_writer_config.tcp_cork_small_file_writes;
    small_direct_options.reactors_per_ip = std::max<std::size_t>(1U, small_writer_config.reactors_per_ip);
    small_direct_options.reactor_count = small_writer_config.reactor_count;
    small_direct_options.max_concurrent_file_transactions = small_writer_config.max_concurrent_file_transactions;
    std::unique_ptr<TargetWriterBackend> small_direct_backend =
        make_target_writer_backend(small_writer_config.target_root, 0U, small_direct_options);
    DirectTargetWriterStats small_direct_writer_stats;
    std::atomic<std::uint64_t> current_small_iops_x100 {0};
    std::atomic<std::uint32_t> bulk_pacing_us {0};
    std::atomic<bool> governor_done {false};

    const auto small_direct_consume_buffer = [&](std::size_t, const BufferHandle& handle) {
        std::uint64_t payload_bytes = 0;
        std::size_t files_written = 0;
        try {
            const DataBuffer& buffer = data_buffer(data_pool, handle);
            if (!hypersync::is_packed_small_file_buffer(buffer)) {
                throw std::runtime_error("small mixed DataWriter-NFS expected packed-small-file buffer");
            }
            std::vector<TargetWriterBackend::WriteChunk> files;
            files.reserve(hypersync::packed_small_file_count(buffer));
            const bool ok = hypersync::visit_packed_small_files(buffer, [&](hypersync::PackedSmallFileView view) {
                FileSpec file;
                file.rel_path = std::string(view.rel_path);
                file.declared_size = view.file_size;
                file.mtime = view.mtime;
                file.mode = view.mode != 0U ? view.mode : 0644U;
                file.uid = view.uid;
                file.gid = view.gid;
                TargetWriterBackend::WriteChunk chunk;
                chunk.spec = std::move(file);
                chunk.data = view.data;
                chunk.offset = 0;
                chunk.last_chunk = true;
                payload_bytes += view.data.size();
                files.push_back(std::move(chunk));
            });
            if (!ok) {
                throw std::runtime_error("small mixed DataWriter-NFS received malformed packed-small-file buffer");
            }
            files_written = files.size();
            small_direct_backend->write_files(files);
            small_direct_writer_stats.buffers_processed.fetch_add(1U, std::memory_order_relaxed);
            small_direct_writer_stats.files_written.fetch_add(files_written, std::memory_order_relaxed);
            small_direct_writer_stats.bytes_written.fetch_add(payload_bytes, std::memory_order_relaxed);
            data_pool.release(handle);
            return true;
        } catch (...) {
            small_direct_writer_stats.files_failed.fetch_add(1U, std::memory_order_relaxed);
            data_pool.release(handle);
            throw;
        }
    };

    auto make_file_provider = [](DataReadFileQueue& queue,
                                 SplitDataReadRoute route,
                                 std::size_t batch_size,
                                 const std::atomic<std::uint32_t>* pacing_us = nullptr) {
        return [&queue, route, batch_size, pacing_us]() {
            if (pacing_us != nullptr) {
                const std::uint32_t delay_us = pacing_us->load(std::memory_order_relaxed);
                if (delay_us != 0U) {
                    std::this_thread::sleep_for(std::chrono::microseconds(delay_us));
                }
            }
            thread_local std::deque<FileSpec> worker_file_batch;
            if (worker_file_batch.empty()) {
                std::vector<FileSpec> next_batch = take_data_file_work_batch(queue, batch_size);
                for (auto& file : next_batch) {
                    worker_file_batch.push_back(std::move(file));
                }
            }
            if (worker_file_batch.empty()) {
                return std::optional<FileSpec> {};
            }
            FileSpec file = std::move(worker_file_batch.front());
            worker_file_batch.pop_front();
            current_split_data_read_route = route;
            return std::optional<FileSpec> {std::move(file)};
        };
    };

    NfsDataBufferReaderJob small_reader_job(
        small_data_config,
        data_pool,
        NfsDataBufferReaderJob::BufferConsumer(small_direct_consume_buffer),
        make_file_provider(ready_queues.small, SplitDataReadRoute::Small, 128U),
        [&ready_queues]() {
            return data_read_timer_expired(ready_queues.small);
        });
    small_reader_job.set_bytes_read_callback([&stats](std::uint64_t bytes_read) {
        record_small_data_read_bytes(stats, bytes_read);
    });
    small_reader_job.set_file_read_callback([&stats]() {
        record_small_data_read_file(stats);
    });
    small_reader_job.set_file_failed_callback([&stats](const FileSpec&) {
        stats.files_failed.fetch_add(1U, std::memory_order_relaxed);
    });

    NfsDataBufferReaderJob large_reader_job(
        large_data_config,
        data_pool,
        large_reader_to_writer,
        make_file_provider(ready_queues.large, SplitDataReadRoute::Large, 128U, &bulk_pacing_us),
        [&ready_queues]() {
            return data_read_timer_expired(ready_queues.large);
        });
    large_reader_job.set_bytes_read_callback([&stats](std::uint64_t bytes_read) {
        record_large_data_read_bytes(stats, bytes_read);
    });
    large_reader_job.set_file_read_callback([&stats]() {
        record_large_data_read_file(stats);
    });
    large_reader_job.set_file_failed_callback([&stats](const FileSpec&) {
        stats.files_failed.fetch_add(1U, std::memory_order_relaxed);
    });

    NfsDataBufferReaderJob medium_reader_job(
        medium_data_config,
        data_pool,
        medium_reader_to_writer,
        make_file_provider(medium_ready_queue, SplitDataReadRoute::Large, 128U, &bulk_pacing_us),
        [&medium_ready_queue]() {
            return data_read_timer_expired(medium_ready_queue);
        });
    medium_reader_job.set_bytes_read_callback([&stats](std::uint64_t bytes_read) {
        record_large_data_read_bytes(stats, bytes_read);
    });
    medium_reader_job.set_file_read_callback([&stats]() {
        record_large_data_read_file(stats);
    });
    medium_reader_job.set_file_failed_callback([&stats](const FileSpec&) {
        stats.files_failed.fetch_add(1U, std::memory_order_relaxed);
    });

    TargetDataWriterJob medium_writer_job(medium_writer_config, data_pool, medium_reader_to_writer);
    TargetDataWriterJob large_writer_job(large_writer_config, data_pool, large_reader_to_writer);

    const auto started = std::chrono::steady_clock::now();
    stats.started_at = started;
    stats.last_print_at = started;
    if (max_duration_seconds > 0.0) {
        const auto stop_at = started +
                             std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                 std::chrono::duration<double>(max_duration_seconds));
        scan_folder_queue.stop_at = stop_at;
        folder_batch_queue.stop_at = stop_at;
        ready_queues.small.stop_at = stop_at;
        medium_ready_queue.stop_at = stop_at;
        ready_queues.large.stop_at = stop_at;
    }

    std::mutex stop_timer_mutex;
    std::condition_variable stop_timer_cv;
    bool cancel_stop_timer = false;
    std::thread stop_timer;
    if (scan_folder_queue.stop_at.has_value()) {
        const auto stop_at = *scan_folder_queue.stop_at;
        stop_timer = std::thread([&]() {
            std::unique_lock<std::mutex> lock(stop_timer_mutex);
            const bool cancelled = stop_timer_cv.wait_until(lock, stop_at, [&]() {
                return cancel_stop_timer;
            });
            if (!cancelled) {
                request_flat_folder_stop(scan_folder_queue);
                request_folder_ready_stop(folder_batch_queue);
                request_data_file_stop(ready_queues.small);
                request_data_file_stop(medium_ready_queue);
                request_data_file_stop(ready_queues.large);
                request_ready_file_spillway_stop(medium_spillway);
                request_ready_file_spillway_stop(large_spillway);
            }
        });
    }

    std::thread stats_printer(run_mixed_folder_ready_write_stats_printer,
                              std::ref(stats),
                              std::ref(ready_queues.small),
                              std::ref(medium_ready_queue),
                              std::ref(ready_queues.large),
                              std::ref(medium_spillway),
                              std::ref(large_spillway),
                              std::cref(current_small_iops_x100),
                              std::cref(bulk_pacing_us),
                              std::cref(small_direct_writer_stats),
                              std::cref(medium_writer_job),
                              std::cref(large_writer_job));

    std::thread governor([&]() {
        constexpr double kTargetSmallIops = 75'000.0;
        constexpr std::uint32_t kInitialDelayUs = 5U;
        constexpr std::uint32_t kMaxDelayUs = 2'000U;
        constexpr std::chrono::milliseconds kSampleInterval(200);
        std::uint64_t previous_files = small_direct_writer_stats.files_written.load(std::memory_order_acquire);
        double smoothed_iops = 0.0;
        while (!governor_done.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(kSampleInterval);
            const std::uint64_t current_files =
                small_direct_writer_stats.files_written.load(std::memory_order_acquire);
            const std::uint64_t delta_files = current_files - previous_files;
            previous_files = current_files;
            const double instant_iops =
                static_cast<double>(delta_files) * 1000.0 /
                static_cast<double>(std::chrono::duration_cast<std::chrono::milliseconds>(kSampleInterval).count());
            smoothed_iops = smoothed_iops == 0.0 ? instant_iops : (smoothed_iops * 0.65 + instant_iops * 0.35);
            current_small_iops_x100.store(static_cast<std::uint64_t>(std::max(0.0, smoothed_iops) * 100.0),
                                          std::memory_order_relaxed);

            const std::uint64_t small_found = stats.small_files_found.load(std::memory_order_acquire);
            const std::uint64_t small_written = current_files;
            const std::size_t queued_small = queued_data_read_files(ready_queues.small);
            const bool small_backlogged =
                queued_small != 0U ||
                small_found > small_written + std::max<std::uint64_t>(1024U, small_data_threads);

            std::uint32_t delay_us = bulk_pacing_us.load(std::memory_order_relaxed);
            if (small_backlogged && smoothed_iops < kTargetSmallIops) {
                delay_us = delay_us == 0U ? kInitialDelayUs : std::min<std::uint32_t>(kMaxDelayUs, delay_us * 2U);
            } else if (delay_us != 0U) {
                delay_us = delay_us <= kInitialDelayUs ? 0U : delay_us / 2U;
            }
            bulk_pacing_us.store(delay_us, std::memory_order_relaxed);
        }
    });

    std::vector<std::thread> spillway_drainers;
    spillway_drainers.emplace_back([&]() {
        try {
            while (true) {
                std::vector<FileSpec> files = take_ready_file_spill_batch(medium_spillway, 4096U);
                if (files.empty()) {
                    break;
                }
                if (!enqueue_data_read_files(medium_ready_queue, std::move(files))) {
                    break;
                }
            }
        } catch (...) {
            const std::exception_ptr error = std::current_exception();
            fail_ready_file_spillway(medium_spillway, error);
            fail_data_file_work(medium_ready_queue, error);
        }
    });
    spillway_drainers.emplace_back([&]() {
        try {
            while (true) {
                std::vector<FileSpec> files = take_ready_file_spill_batch(large_spillway, 4096U);
                if (files.empty()) {
                    break;
                }
                if (!enqueue_data_read_files(ready_queues.large, std::move(files))) {
                    break;
                }
            }
        } catch (...) {
            const std::exception_ptr error = std::current_exception();
            fail_ready_file_spillway(large_spillway, error);
            fail_data_file_work(ready_queues.large, error);
        }
    });

    const std::size_t folder_create_threads = target_data_writer_effective_worker_count(writer_config);
    std::vector<std::thread> folder_workers;
    folder_workers.reserve(folder_create_threads);
    for (std::size_t index = 0; index < folder_create_threads; ++index) {
        folder_workers.emplace_back([&, index]() {
            TargetWriterBackend::Options options;
            options.preserve_metadata = false;
            options.fsync_on_finish = false;
            options.ensure_parent_directories = false;
            options.max_concurrent_file_transactions =
                std::max<std::size_t>(1U, writer_config.max_concurrent_file_transactions);
            auto backend = make_target_writer_backend(writer_config.target_root, index, options);
            try {
                while (true) {
                    std::optional<FolderReadyFileBatch> batch = take_folder_ready_batch(folder_batch_queue);
                    if (!batch.has_value()) {
                        break;
                    }
                    if (!batch->directories.empty()) {
                        backend->ensure_directories(batch->directories);
                        folders_created.fetch_add(batch->directories.size(), std::memory_order_relaxed);
                    }
                    if (!batch->files.empty()) {
                        std::vector<FileSpec> small_files;
                        std::vector<FileSpec> medium_files;
                        std::vector<FileSpec> large_files;
                        std::size_t small_count = 0;
                        std::size_t medium_count = 0;
                        std::size_t large_count = 0;
                        std::uint64_t small_bytes = 0;
                        std::uint64_t medium_bytes = 0;
                        std::uint64_t large_bytes = 0;
                        classify_ready_files(true_small_threshold,
                                             medium_threshold,
                                             std::move(batch->files),
                                             small_files,
                                             medium_files,
                                             large_files,
                                             small_bytes,
                                             medium_bytes,
                                             large_bytes);
                        small_count = small_files.size();
                        medium_count = medium_files.size();
                        large_count = large_files.size();
                        if (!enqueue_data_read_files(ready_queues.small, std::move(small_files)) ||
                            !try_enqueue_data_read_files_or_spill(medium_ready_queue,
                                                                  medium_spillway,
                                                                  std::move(medium_files)) ||
                            !try_enqueue_data_read_files_or_spill(ready_queues.large,
                                                                  large_spillway,
                                                                  std::move(large_files))) {
                            break;
                        }
                        stats.small_files_found.fetch_add(small_count, std::memory_order_relaxed);
                        stats.large_files_found.fetch_add(medium_count + large_count, std::memory_order_relaxed);
                        stats.small_logical_size_bytes.fetch_add(small_bytes, std::memory_order_relaxed);
                        stats.large_logical_size_bytes.fetch_add(medium_bytes + large_bytes, std::memory_order_relaxed);
                    }
                }
            } catch (...) {
                const std::exception_ptr error = std::current_exception();
                fail_folder_ready_work(folder_batch_queue, error);
                fail_data_file_work(ready_queues.small, error);
                fail_data_file_work(medium_ready_queue, error);
                fail_data_file_work(ready_queues.large, error);
                fail_ready_file_spillway(medium_spillway, error);
                fail_ready_file_spillway(large_spillway, error);
            }
        });
    }

    medium_writer_job.start();
    large_writer_job.start();
    small_reader_job.start();
    medium_reader_job.start();
    large_reader_job.start();

    const std::size_t metadata_threads = std::max<std::size_t>(1U, meta_config.worker_count);
    std::vector<std::thread> metadata_workers;
    metadata_workers.reserve(metadata_threads);
    DataReadFileQueue unused_ready_queue;
    for (std::size_t index = 0; index < metadata_threads; ++index) {
        metadata_workers.emplace_back(scan_folder_ready_metadata_worker,
                                      meta_config.source_root,
                                      meta_config.recursive,
                                      std::max<std::size_t>(1U, meta_config.async_directory_depth),
                                      meta_config.readdirplus_page_bytes,
                                      0U,
                                      0U,
                                      std::ref(scan_folder_queue),
                                      std::ref(folder_batch_queue),
                                      std::ref(unused_ready_queue),
                                      std::ref(stats));
    }

    std::exception_ptr pipeline_error;
    for (auto& worker : metadata_workers) {
        worker.join();
    }
    mark_folder_ready_input_done(folder_batch_queue);
    for (auto& worker : folder_workers) {
        worker.join();
    }
    mark_ready_file_spillway_input_done(medium_spillway);
    mark_ready_file_spillway_input_done(large_spillway);
    for (auto& drainer : spillway_drainers) {
        drainer.join();
    }
    mark_data_file_input_done(ready_queues.small);
    mark_data_file_input_done(medium_ready_queue);
    mark_data_file_input_done(ready_queues.large);

    try {
        small_reader_job.wait();
        medium_reader_job.wait();
        large_reader_job.wait();
        medium_writer_job.wait();
        large_writer_job.wait();
    } catch (...) {
        pipeline_error = std::current_exception();
        try {
            small_reader_job.stop();
        } catch (...) {}
        try {
            large_reader_job.stop();
        } catch (...) {}
        try {
            medium_reader_job.stop();
        } catch (...) {}
        try {
            medium_writer_job.stop();
        } catch (...) {}
        try {
            large_writer_job.stop();
        } catch (...) {}
    }

    governor_done.store(true, std::memory_order_release);
    if (governor.joinable()) {
        governor.join();
    }

    stats.printer_done.store(true, std::memory_order_relaxed);
    stats.printer_cv.notify_all();
    if (stats_printer.joinable()) {
        stats_printer.join();
    }

    {
        std::lock_guard<std::mutex> lock(stop_timer_mutex);
        cancel_stop_timer = true;
    }
    stop_timer_cv.notify_all();
    if (stop_timer.joinable()) {
        stop_timer.join();
    }

    if (scan_folder_queue.error) {
        std::rethrow_exception(scan_folder_queue.error);
    }
    if (folder_batch_queue.error) {
        std::rethrow_exception(folder_batch_queue.error);
    }
    if (ready_queues.small.error) {
        std::rethrow_exception(ready_queues.small.error);
    }
    if (medium_ready_queue.error) {
        std::rethrow_exception(medium_ready_queue.error);
    }
    if (ready_queues.large.error) {
        std::rethrow_exception(ready_queues.large.error);
    }
    if (medium_spillway.error) {
        std::rethrow_exception(medium_spillway.error);
    }
    if (large_spillway.error) {
        std::rethrow_exception(large_spillway.error);
    }
    if (pipeline_error) {
        std::rethrow_exception(pipeline_error);
    }

    const TargetWriterStats small_stats = small_direct_writer_stats.snapshot();
    const TargetWriterStats medium_stats = medium_writer_job.stats();
    const TargetWriterStats large_stats = large_writer_job.stats();
    writer_stats.worker_count = medium_writer_threads + large_writer_threads;
    writer_stats.buffers_processed =
        small_stats.buffers_processed + medium_stats.buffers_processed + large_stats.buffers_processed;
    writer_stats.files_written =
        small_stats.files_written + medium_stats.files_written + large_stats.files_written;
    writer_stats.files_failed =
        small_stats.files_failed + medium_stats.files_failed + large_stats.files_failed;
    writer_stats.bytes_written =
        small_stats.bytes_written + medium_stats.bytes_written + large_stats.bytes_written;
    writer_stats.folders_written = folders_created.load(std::memory_order_relaxed);
    queue_capacity = folder_batch_queue.max_entries;
    queue_high_watermark = folder_batch_queue.high_watermark;

    DataReadBenchmarkSnapshot snapshot = snapshot_data_read_stats(stats);
    snapshot.folders_written = static_cast<std::size_t>(writer_stats.folders_written);
    snapshot.folders_per_second = snapshot.elapsed_seconds > 0.0
                                      ? static_cast<double>(snapshot.folders_written) / snapshot.elapsed_seconds
                                      : 0.0;
    snapshot.data_buffer_slots = pool_slots;
    snapshot.data_queue_depth =
        ready_queues.small.max_entries + medium_ready_queue.max_entries + ready_queues.large.max_entries;
    return snapshot;
}

DataReadBenchmarkSnapshot run_parallel_mkdir_only_scan(const NfsMetaReaderConfig& meta_config,
                                                       const TargetMetaWriterConfig& writer_config,
                                                       std::size_t metadata_buffer_slots,
                                                       double max_duration_seconds,
                                                       TargetWriterStats& writer_stats,
                                                       std::size_t& metadata_queue_capacity,
                                                       std::size_t& metadata_queue_high_watermark) {
    const std::size_t metadata_threads = std::max<std::size_t>(1U, meta_config.worker_count);
    const std::size_t flat_slots =
        std::max<std::size_t>(1U, metadata_buffer_slots == 0U ? kMetadataDiscardDefaultBufferSlots
                                                              : metadata_buffer_slots);
    RawBufferPool metadata_pool = make_metadata_batch_buffer_pool(flat_slots);
    RawBufferPool folder_pool = make_folder_work_buffer_pool(
        kFolderWorkBufferPoolId,
        folder_work_slots(metadata_threads, meta_config.async_directory_depth));
    RawBufferPool folder_feedback_pool = make_folder_work_buffer_pool(
        kFolderFeedbackBufferPoolId,
        folder_work_slots(metadata_threads, meta_config.async_directory_depth));

    BufQueue metadata_queue(flat_slots);
    BufQueue folder_queue(folder_pool.capacity());
    BufQueue folder_feedback_queue(folder_feedback_pool.capacity());

    FolderSeederJob folder_seeder(meta_config.recursive,
                                  max_duration_seconds,
                                  folder_pool,
                                  folder_feedback_pool,
                                  folder_queue,
                                  folder_feedback_queue);
    NfsMetaReaderBufferJob scanner(meta_config.source_root,
                                   "size",
                                   metadata_threads,
                                   std::max<std::size_t>(1U, meta_config.async_directory_depth),
                                   folder_pool,
                                   folder_feedback_pool,
                                   folder_queue,
                                   folder_feedback_queue,
                                   metadata_pool,
                                   metadata_queue,
                                   max_duration_seconds);
    TargetMetaWriterJob writer(writer_config, metadata_pool, metadata_queue);

    const auto started = std::chrono::steady_clock::now();
    folder_seeder.start();
    writer.start();
    scanner.start();

    scanner.wait();
    folder_seeder.wait();
    if (auto error = folder_seeder.error()) {
        std::rethrow_exception(error);
    }
    writer.wait();

    writer_stats = writer.stats();
    metadata_queue_capacity = metadata_queue.capacity();
    metadata_queue_high_watermark = metadata_queue.high_watermark();

    const DistributedDiffRunReport scanner_stats = scanner.stats();
    DataReadBenchmarkSnapshot snapshot;
    snapshot.files_found = static_cast<std::size_t>(scanner_stats.files_compared);
    snapshot.folders_found = static_cast<std::size_t>(scanner_stats.folders_sent);
    snapshot.logical_size_bytes = scanner_stats.source_logical_size_bytes;
    snapshot.folders_written = static_cast<std::size_t>(writer_stats.folders_written);
    snapshot.elapsed_seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    if (snapshot.elapsed_seconds > 0.0) {
        snapshot.files_per_second = static_cast<double>(snapshot.files_found) / snapshot.elapsed_seconds;
        snapshot.folders_per_second = static_cast<double>(snapshot.folders_written) / snapshot.elapsed_seconds;
    }
    return snapshot;
}

DataReadBenchmarkSnapshot run_parallel_split_data_read_scan(const NfsMetaReaderConfig& meta_config,
                                                            NfsDataReaderConfig small_data_config,
                                                            NfsDataReaderConfig large_data_config,
                                                            std::uint64_t small_file_threshold,
                                                            std::size_t max_files_queued,
                                                            std::size_t small_max_files_queued,
                                                            std::size_t large_max_files_queued,
                                                            bool dual_scan_small_large,
                                                            bool recon_scan_enabled,
                                                            std::size_t recon_meta_reader_threads,
                                                            std::size_t recon_metadata_async_depth,
                                                            std::uint64_t recon_page_sleep_us,
                                                            bool morph_large_readers_to_small,
                                                            bool bucket_priority_enabled,
                                                            std::size_t small_meta_reader_threads,
                                                            std::size_t large_meta_reader_threads,
                                                            std::size_t data_buffer_slots,
                                                            std::size_t data_queue_depth,
                                                            double max_duration_seconds,
                                                            std::uint32_t stats_interval_seconds,
                                                            const SplitDataReadAutoscaleConfig& autoscale_config,
                                                            const std::filesystem::path& status_socket_path) {
    FlatMetadataWorkQueue folder_queue;
    folder_queue.folders.push_back(FileSpec{});
    FlatMetadataWorkQueue small_folder_queue;
    FlatMetadataWorkQueue large_folder_queue;
    FlatMetadataWorkQueue recon_folder_queue;
    if (dual_scan_small_large) {
        small_folder_queue.folders.push_back(FileSpec{});
        large_folder_queue.folders.push_back(FileSpec{});
    }
    if (recon_scan_enabled) {
        recon_folder_queue.folders.push_back(FileSpec{});
    }
    SplitDataReadFileQueues file_queues;
    file_queues.small.max_entries =
        std::max<std::size_t>(1, small_max_files_queued == 0U ? max_files_queued : small_max_files_queued);
    file_queues.large.max_entries =
        std::max<std::size_t>(1, large_max_files_queued == 0U ? max_files_queued : large_max_files_queued);
    file_queues.small.resume_entries = file_queues.small.max_entries * 4U / 5U;
    file_queues.small_file_threshold = small_file_threshold == 0U ? 128U * 1024U : small_file_threshold;

    DataReadBenchmarkStats stats;
    ReconScanStats recon_stats;
    stats.print_interval_seconds = std::max<std::uint32_t>(1, stats_interval_seconds);
    stats.folders_found.store(1, std::memory_order_relaxed);
    reset_nfs_async_read_latency_metrics();

    const std::size_t small_threads = std::max<std::size_t>(1, small_data_config.data_reader_worker_count);
    const std::size_t large_threads = std::max<std::size_t>(1, large_data_config.data_reader_worker_count);
    const std::size_t outstanding = std::max<std::size_t>(1, small_data_config.outstanding_requests) +
                                    std::max<std::size_t>(1, large_data_config.outstanding_requests);
    const std::size_t metadata_threads = std::max<std::size_t>(1, meta_config.worker_count);
    const std::size_t small_metadata_threads =
        std::max<std::size_t>(1, small_meta_reader_threads == 0U ? metadata_threads : small_meta_reader_threads);
    const std::size_t large_metadata_threads =
        std::max<std::size_t>(1, large_meta_reader_threads == 0U ? metadata_threads : large_meta_reader_threads);
    const std::size_t hardware_threads =
        std::max<std::size_t>(1, std::thread::hardware_concurrency());
    const std::size_t default_recon_metadata_threads =
        bucket_priority_enabled
            ? std::max<std::size_t>(small_metadata_threads,
                                    std::min<std::size_t>(256U, hardware_threads * 2U))
            : std::size_t {1};
    const std::size_t default_recon_async_depth =
        bucket_priority_enabled
            ? std::max<std::size_t>(1, meta_config.async_directory_depth)
            : std::size_t {1};
    const std::size_t recon_metadata_threads =
        std::max<std::size_t>(
            1,
            recon_meta_reader_threads == 0U ? default_recon_metadata_threads
                                            : recon_meta_reader_threads);
    const std::size_t recon_async_depth =
        std::max<std::size_t>(
            1,
            recon_metadata_async_depth == 0U ? default_recon_async_depth
                                             : recon_metadata_async_depth);
    const std::size_t recon_steady_metadata_threads =
        bucket_priority_enabled
            ? std::min<std::size_t>(
                  recon_metadata_threads,
                  std::max<std::size_t>(8U, hardware_threads / 5U))
            : recon_metadata_threads;
    const std::size_t large_scanner_floor =
        bucket_priority_enabled && large_metadata_threads > 8U ? std::size_t {8} : std::size_t {1};
    const std::size_t scanner_shift_capacity =
        bucket_priority_enabled && large_metadata_threads > large_scanner_floor
            ? large_metadata_threads - large_scanner_floor
            : std::size_t {0};
    const std::size_t small_metadata_worker_slots =
        small_metadata_threads + scanner_shift_capacity;
    const std::size_t large_metadata_worker_slots = large_metadata_threads;
    const std::size_t total_reader_threads = small_threads + large_threads;
    const std::size_t queue_depth =
        std::max<std::size_t>(1, data_queue_depth == 0 ? total_reader_threads * outstanding * 2U
                                                       : data_queue_depth);
    const std::size_t pool_slots =
        std::max<std::size_t>(total_reader_threads * outstanding + queue_depth * 2U + 1U,
                              data_buffer_slots == 0 ? total_reader_threads * outstanding * 3U + 1U
                                                     : data_buffer_slots);

    RawBufferPool data_pool = make_data_buffer_pool(pool_slots);
    BufferPoolRegistry registry;
    registry.register_pool(data_pool);
    BufQueue small_reader_to_discard(queue_depth);
    BufQueue large_reader_to_discard(queue_depth);

    small_data_config.small_file_threshold = file_queues.small_file_threshold;
    large_data_config.small_file_threshold = 0U;
    large_data_config.pack_small_files = false;
    large_data_config.small_file_async_window = 1U;

    auto make_file_provider = [](DataReadFileQueue& queue, SplitDataReadRoute route) {
        return [&queue, route]() {
            thread_local std::deque<FileSpec> worker_file_batch;
            if (worker_file_batch.empty()) {
                std::vector<FileSpec> next_batch = take_data_file_work_batch(queue, 128);
                for (auto& file : next_batch) {
                    worker_file_batch.push_back(std::move(file));
                }
            }
            if (worker_file_batch.empty()) {
                return std::optional<FileSpec> {};
            }
            FileSpec file = std::move(worker_file_batch.front());
            worker_file_batch.pop_front();
            current_split_data_read_route = route;
            return std::optional<FileSpec> {std::move(file)};
        };
    };

    std::atomic<std::uint32_t> large_reader_small_priority_percent {0};
    ScannerCapacityControl small_scanner_capacity;
    ScannerCapacityControl large_scanner_capacity;
    ScannerCapacityControl recon_scanner_capacity;
    small_scanner_capacity.active_workers.store(small_metadata_threads, std::memory_order_relaxed);
    large_scanner_capacity.active_workers.store(large_metadata_threads, std::memory_order_relaxed);
    recon_scanner_capacity.active_workers.store(recon_metadata_threads, std::memory_order_relaxed);
    auto make_morphing_large_file_provider =
        [](DataReadFileQueue& large_queue,
           DataReadFileQueue& small_queue,
           bool morph_to_small,
           std::atomic<std::uint32_t>& small_priority_percent) {
            return [&large_queue, &small_queue, morph_to_small, &small_priority_percent]() {
                thread_local std::deque<std::pair<FileSpec, SplitDataReadRoute>> worker_file_batch;
                if (worker_file_batch.empty()) {
                    thread_local std::uint64_t priority_counter = 0;
                    const std::uint32_t priority =
                        morph_to_small
                            ? std::min<std::uint32_t>(
                                  100U,
                                  small_priority_percent.load(std::memory_order_relaxed))
                            : 0U;
                    const bool try_small_first =
                        priority != 0U && (priority == 100U || (priority_counter++ % 100U) < priority);
                    if (try_small_first) {
                        std::vector<FileSpec> next_batch = take_data_file_work_batch(small_queue, 128);
                        for (auto& file : next_batch) {
                            worker_file_batch.emplace_back(std::move(file), SplitDataReadRoute::Small);
                        }
                    }
                    if (worker_file_batch.empty()) {
                        std::vector<FileSpec> next_batch = take_data_file_work_batch(large_queue, 128);
                        for (auto& file : next_batch) {
                            worker_file_batch.emplace_back(std::move(file), SplitDataReadRoute::Large);
                        }
                    }
                    if (worker_file_batch.empty() && morph_to_small && data_file_input_done_and_empty(large_queue)) {
                        std::vector<FileSpec> next_batch = take_data_file_work_batch(small_queue, 128);
                        for (auto& file : next_batch) {
                            worker_file_batch.emplace_back(std::move(file), SplitDataReadRoute::Small);
                        }
                    }
                }
                if (worker_file_batch.empty()) {
                    return std::optional<FileSpec> {};
                }
                auto item = std::move(worker_file_batch.front());
                worker_file_batch.pop_front();
                current_split_data_read_route = item.second;
                return std::optional<FileSpec> {std::move(item.first)};
            };
        };

    NfsDataBufferReaderJob small_reader_job(
        small_data_config,
        data_pool,
        small_reader_to_discard,
        make_file_provider(file_queues.small, SplitDataReadRoute::Small),
        [&file_queues]() {
            return data_read_timer_expired(file_queues.small);
        });
    small_reader_job.set_bytes_read_callback([&stats](std::uint64_t bytes_read) {
        record_small_data_read_bytes(stats, bytes_read);
    });
    small_reader_job.set_file_read_callback([&stats]() {
        record_small_data_read_file(stats);
    });
    small_reader_job.set_file_failed_callback([&stats](const FileSpec&) {
        stats.files_failed.fetch_add(1, std::memory_order_relaxed);
    });

    NfsDataBufferReaderJob large_reader_job(
        large_data_config,
        data_pool,
        large_reader_to_discard,
        make_morphing_large_file_provider(file_queues.large,
                                          file_queues.small,
                                          morph_large_readers_to_small,
                                          large_reader_small_priority_percent),
        [&file_queues]() {
            return data_read_timer_expired(file_queues.large) || data_read_timer_expired(file_queues.small);
        });
    large_reader_job.set_bytes_read_callback([&stats](std::uint64_t bytes_read) {
        record_current_split_data_read_bytes(stats, bytes_read);
    });
    large_reader_job.set_file_read_callback([&stats]() {
        record_current_split_data_read_file(stats);
    });
    large_reader_job.set_file_failed_callback([&stats](const FileSpec&) {
        stats.files_failed.fetch_add(1, std::memory_order_relaxed);
    });

    BufferDiscarderJob small_discarder(BufferDiscarderConfig(1), small_reader_to_discard, registry);
    BufferDiscarderJob large_discarder(BufferDiscarderConfig(1), large_reader_to_discard, registry);

    const bool persist_autoscale_profile =
        (autoscale_config.pipeline_enabled || autoscale_config.large_enabled) &&
        !autoscale_config.settings_path.empty() &&
        !autoscale_config.profile_name.empty();
    std::unique_ptr<AutoScaleProfileStore> autoscale_profile_store;
    if (persist_autoscale_profile) {
        autoscale_profile_store = std::make_unique<AutoScaleProfileStore>(autoscale_config.settings_path);
    }

    auto make_reader_policy =
        [&autoscale_config, &autoscale_profile_store](const std::string& job_name,
                                                      std::size_t worker_count,
                                                      std::size_t initial_workers) {
        AutoScalePolicy policy;
        if (autoscale_profile_store) {
            policy = autoscale_profile_store->job_policy(autoscale_config.profile_name,
                                                         job_name,
                                                         std::max<std::size_t>(1, worker_count));
        } else {
            policy.enabled = true;
            policy.min_workers = 1U;
            policy.max_workers = std::max<std::size_t>(1, worker_count);
            policy.initial_workers = std::max<std::size_t>(1, initial_workers);
            policy.cooldown_samples = 1U;
            policy.max_cooldown_samples = std::max<std::uint64_t>(
                1U,
                (5000U + std::max<std::uint64_t>(1U, autoscale_config.interval_ms) - 1U) /
                    std::max<std::uint64_t>(1U, autoscale_config.interval_ms));
        }
        policy.cooldown_samples = 1;
        policy.max_cooldown_samples = std::max<std::uint64_t>(
            1U,
            (5000U + std::max<std::uint64_t>(1U, autoscale_config.interval_ms) - 1U) /
                std::max<std::uint64_t>(1U, autoscale_config.interval_ms));
        policy.scale_up_input_fullness = 0.25;
        policy.scale_down_input_fullness = 0.02;
        policy.output_blocked_fullness = 0.95;
        policy.scale_up_output_fullness_limit = 0.80;
        policy.busy_scale_up = 0.20;
        policy.idle_scale_down = 0.75;
        policy.min_improvement_ratio = 0.02;
        return policy;
    };

    auto make_reader_metrics_provider =
        [&file_queues](DataReadFileQueue& file_queue,
                       BufQueue& output_queue,
                       NfsDataBufferReaderJob& reader_job,
                       std::atomic<std::uint64_t>& bytes_counter) {
            std::deque<std::pair<std::chrono::steady_clock::time_point, std::uint64_t>> byte_samples;
            bool saw_backlog = false;
            return [&file_queues, &file_queue, &output_queue, &reader_job, &bytes_counter,
                    byte_samples, saw_backlog]() mutable {
                (void)file_queues;
                const auto now = std::chrono::steady_clock::now();
                const std::uint64_t bytes = bytes_counter.load(std::memory_order_relaxed);
                byte_samples.emplace_back(now, bytes);
                while (byte_samples.size() > 2U &&
                       std::chrono::duration<double>(now - byte_samples.front().first).count() > 5.0) {
                    byte_samples.pop_front();
                }
                AutoScaleMetrics metrics;
                const std::size_t queued_files = queued_data_read_files(file_queue);
                const double input_fullness = static_cast<double>(queued_files) /
                    static_cast<double>(std::max<std::size_t>(1, file_queue.max_entries));
                const std::size_t active_workers = std::max<std::size_t>(1, reader_job.active_worker_limit());
                saw_backlog = saw_backlog || queued_files != 0U;
                metrics.input_fullness = saw_backlog ? input_fullness : 0.50;
                metrics.input_available_ratio =
                    queued_files >= active_workers ? 1.0 : static_cast<double>(queued_files) /
                                                        static_cast<double>(active_workers);
                metrics.output_fullness = static_cast<double>(output_queue.size()) /
                    static_cast<double>(std::max<std::size_t>(1, output_queue.capacity()));
                const RuntimeMetricsSnapshot runtime = reader_job.runtime_metrics().snapshot();
                if (runtime.total_wall_ns != 0U) {
                    const bool has_backlog = queued_files != 0U;
                    if (has_backlog && metrics.output_fullness < 0.95) {
                        metrics.busy_ratio = 1.0;
                    } else {
                        const std::uint64_t useful_ns =
                            runtime.state_wall_ns[runtime_state_index(RuntimeState::processing)] +
                            runtime.state_wall_ns[runtime_state_index(RuntimeState::wait_io)];
                        metrics.busy_ratio =
                            static_cast<double>(useful_ns) / static_cast<double>(runtime.total_wall_ns);
                    }
                    metrics.wait_input_ratio =
                        saw_backlog
                            ? static_cast<double>(
                                  runtime.state_wall_ns[runtime_state_index(RuntimeState::wait_input_empty)]) /
                                  static_cast<double>(runtime.total_wall_ns)
                            : 0.0;
                    metrics.wait_output_ratio =
                        static_cast<double>(runtime.state_wall_ns[runtime_state_index(RuntimeState::wait_output_full)]) /
                        static_cast<double>(runtime.total_wall_ns);
                }
                if (byte_samples.size() >= 2U) {
                    const auto& oldest = byte_samples.front();
                    const double elapsed = std::chrono::duration<double>(now - oldest.first).count();
                    metrics.throughput_per_second =
                        elapsed > 0.0 ? static_cast<double>(bytes - oldest.second) / elapsed : 0.0;
                }
                return metrics;
            };
        };

    std::unique_ptr<JobAutoScaleRunner> large_autoscaler;
    std::unique_ptr<PipelineAutoScaleRunner> pipeline_autoscaler;
    if (autoscale_config.pipeline_enabled && !bucket_priority_enabled) {
        pipeline_autoscaler = std::make_unique<PipelineAutoScaleRunner>(
            std::vector<PipelineAutoScaleRunner::Stage> {
                PipelineAutoScaleRunner::Stage {
                    "small_data_reader",
                    &small_reader_job,
                    make_reader_policy("small_data_reader", small_reader_job.worker_count(), 1U),
                    make_reader_metrics_provider(file_queues.small,
                                                 small_reader_to_discard,
                                                 small_reader_job,
                                                 stats.small_bytes_read),
                },
                PipelineAutoScaleRunner::Stage {
                    "large_data_reader",
                    &large_reader_job,
                    make_reader_policy("large_data_reader", large_reader_job.worker_count(), 1U),
                    make_reader_metrics_provider(file_queues.large,
                                                 large_reader_to_discard,
                                                 large_reader_job,
                                                 stats.large_bytes_read),
                },
            },
            std::chrono::milliseconds(std::max<std::uint64_t>(100, autoscale_config.interval_ms)));
        pipeline_autoscaler->set_decision_callback([](const PipelineAutoScaleRunner::StageDecision& decision) {
            if (decision.decision.changed || decision.stage_advanced) {
                std::cerr << "pipeline_autoscale stage=" << decision.stage_name
                          << " active_workers=" << decision.decision.active_workers
                          << " reason=" << decision.decision.reason
                          << " advanced=" << (decision.stage_advanced ? "true" : "false") << '\n';
            }
        });
    } else if (autoscale_config.large_enabled && !bucket_priority_enabled) {
        large_autoscaler = std::make_unique<JobAutoScaleRunner>(
            large_reader_job,
            make_reader_policy(
                "large_data_reader",
                large_reader_job.worker_count(),
                autoscale_config.large_initial_workers == 0U
                    ? std::max<std::size_t>(1, large_reader_job.worker_count() / 2U)
                    : autoscale_config.large_initial_workers),
            make_reader_metrics_provider(file_queues.large,
                                         large_reader_to_discard,
                                         large_reader_job,
                                         stats.large_bytes_read),
            std::chrono::milliseconds(std::max<std::uint64_t>(100, autoscale_config.interval_ms)));
        large_autoscaler->set_decision_callback([&large_reader_job](const AutoScaleDecision& decision) {
            if (decision.changed) {
                std::cerr << "autoscale job=large_data_reader active_workers="
                          << decision.active_workers
                          << " max_workers=" << large_reader_job.worker_count()
                          << " reason=" << decision.reason << '\n';
            }
        });
    }

    struct PrioritySample {
        std::chrono::steady_clock::time_point at;
        std::uint64_t small_read = 0;
        std::uint64_t large_read = 0;
        std::uint64_t bytes_read = 0;
        std::uint64_t large_bytes_read = 0;
    };
    struct BucketTotals {
        std::uint64_t small_total = 0;
        std::uint64_t large_total = 0;
        std::uint64_t large_total_bytes = 0;
        const char* source = "production";
    };
    std::atomic<bool> production_scan_completed {false};
    std::atomic<bool> bucket_priority_stop {false};
    std::thread bucket_priority_coordinator;
    if (bucket_priority_enabled) {
        bucket_priority_coordinator = std::thread([&]() {
            std::deque<PrioritySample> samples;
            const auto sample_window = std::chrono::minutes(1);
            const auto recon_startup_window = std::chrono::minutes(2);
            const auto coordinator_started_at = std::chrono::steady_clock::now();
            auto last_human_report = std::chrono::steady_clock::time_point {};
            std::uint64_t small_low_watermark_intervals = 0;

            AutoScalePolicy small_lane_policy;
            small_lane_policy.enabled = true;
            small_lane_policy.min_workers =
                std::max<std::size_t>(1, std::min<std::size_t>(96, small_reader_job.worker_count()));
            small_lane_policy.max_workers = std::max(small_lane_policy.min_workers,
                                                     small_reader_job.worker_count());
            small_lane_policy.initial_workers =
                std::clamp<std::size_t>(small_reader_job.active_worker_limit(),
                                        small_lane_policy.min_workers,
                                        small_lane_policy.max_workers);
            small_lane_policy.cooldown_samples = 1;
            small_lane_policy.max_cooldown_samples = 5;
            small_lane_policy.initial_probe_step_ratio = 0.125;
            small_lane_policy.min_probe_step_ratio = 0.125;

            AutoScalePolicy large_lane_policy;
            large_lane_policy.enabled = true;
            large_lane_policy.min_workers = 1;
            large_lane_policy.max_workers = std::max<std::size_t>(1, large_reader_job.worker_count());
            large_lane_policy.initial_workers =
                std::clamp<std::size_t>(large_reader_job.active_worker_limit(),
                                        large_lane_policy.min_workers,
                                        large_lane_policy.max_workers);
            large_lane_policy.cooldown_samples = 1;
            large_lane_policy.max_cooldown_samples = 5;
            large_lane_policy.initial_probe_step_ratio = 0.125;
            large_lane_policy.min_probe_step_ratio = 0.125;

            AutoScaler small_lane_scaler(small_lane_policy);
            AutoScaler large_lane_scaler(large_lane_policy);
            small_reader_job.set_active_worker_limit(small_lane_scaler.active_workers());
            large_reader_job.set_active_worker_limit(large_lane_scaler.active_workers());
            while (!bucket_priority_stop.load(std::memory_order_relaxed)) {
                const auto now = std::chrono::steady_clock::now();
                const DataReadBenchmarkSnapshot snapshot =
                    snapshot_data_read_stats(stats, recon_scan_enabled ? &recon_stats : nullptr);
                samples.push_back(PrioritySample {
                    now,
                    static_cast<std::uint64_t>(snapshot.small_files_read),
                    static_cast<std::uint64_t>(snapshot.large_files_read),
                    snapshot.bytes_read,
                    snapshot.large_bytes_read,
                });
                while (samples.size() > 2U && now - samples.front().at > sample_window) {
                    samples.pop_front();
                }

                double small_rate = snapshot.small_files_per_second;
                double large_rate = snapshot.large_files_per_second;
                double large_byte_rate = snapshot.large_bytes_read > 0U && snapshot.elapsed_seconds > 0.0
                                             ? static_cast<double>(snapshot.large_bytes_read) /
                                                   snapshot.elapsed_seconds
                                             : 0.0;
                double total_byte_rate = snapshot.bytes_read > 0U && snapshot.elapsed_seconds > 0.0
                                             ? static_cast<double>(snapshot.bytes_read) /
                                                   snapshot.elapsed_seconds
                                             : 0.0;
                if (samples.size() >= 2U) {
                    const auto& oldest = samples.front();
                    const double elapsed = std::chrono::duration<double>(now - oldest.at).count();
                    if (elapsed > 0.0) {
                        small_rate = static_cast<double>(
                                         snapshot.small_files_read - oldest.small_read) / elapsed;
                        large_rate = static_cast<double>(
                                         snapshot.large_files_read - oldest.large_read) / elapsed;
                        total_byte_rate = static_cast<double>(
                                              snapshot.bytes_read - oldest.bytes_read) / elapsed;
                        large_byte_rate = static_cast<double>(
                                              snapshot.large_bytes_read - oldest.large_bytes_read) / elapsed;
                    }
                }

                const std::size_t queued_small = queued_data_read_files(file_queues.small);
                const std::size_t queued_large = queued_data_read_files(file_queues.large);
                BucketTotals displayed_totals;
                if (recon_scan_enabled) {
                    displayed_totals.source = "recon";
                    displayed_totals.small_total =
                        static_cast<std::uint64_t>(snapshot.recon_small_files_found);
                    displayed_totals.large_total =
                        static_cast<std::uint64_t>(snapshot.recon_large_files_found);
                    displayed_totals.large_total_bytes = snapshot.recon_large_logical_size_bytes;
                } else {
                    displayed_totals.small_total =
                        static_cast<std::uint64_t>(snapshot.small_files_found);
                    displayed_totals.large_total =
                        static_cast<std::uint64_t>(snapshot.large_files_found);
                    displayed_totals.large_total_bytes = snapshot.large_logical_size_bytes;
                }
                const std::uint64_t control_small_total =
                    std::max<std::uint64_t>(displayed_totals.small_total,
                                            static_cast<std::uint64_t>(snapshot.small_files_read));
                const std::uint64_t control_large_total =
                    std::max<std::uint64_t>(displayed_totals.large_total,
                                            static_cast<std::uint64_t>(snapshot.large_files_read));
                const std::uint64_t control_large_total_bytes =
                    std::max<std::uint64_t>(displayed_totals.large_total_bytes,
                                            snapshot.large_bytes_read);
                const std::uint64_t small_total =
                    std::max<std::uint64_t>(control_small_total,
                                            static_cast<std::uint64_t>(snapshot.small_files_found));
                const std::uint64_t large_total =
                    std::max<std::uint64_t>(control_large_total,
                                            static_cast<std::uint64_t>(snapshot.large_files_found));
                const std::uint64_t large_total_bytes =
                    std::max<std::uint64_t>(control_large_total_bytes,
                                            snapshot.large_logical_size_bytes);

                SplitBucketPriorityInput input;
                input.small_total = small_total;
                input.large_total = large_total;
                input.large_total_bytes = large_total_bytes;
                input.small_done = snapshot.small_files_read;
                input.large_done = snapshot.large_files_read;
                input.large_done_bytes = snapshot.large_bytes_read;
                input.small_files_per_second = small_rate;
                input.large_files_per_second = large_rate;
                input.large_bytes_per_second = large_byte_rate;
                input.current_small_workers = small_reader_job.active_worker_limit();
                input.current_large_workers = large_reader_job.active_worker_limit();
                input.max_small_workers = small_reader_job.worker_count();
                input.max_large_workers = large_reader_job.worker_count();
                const SplitBucketPriorityDecision decision =
                    choose_split_bucket_priority_workers_impl(input);

                if (queued_small < file_queues.small.resume_entries) {
                    ++small_low_watermark_intervals;
                } else {
                    small_low_watermark_intervals = 0;
                }

                BucketPathOverloadInput overload_input;
                overload_input.small_eta_seconds = decision.small_eta_seconds;
                overload_input.large_eta_seconds = decision.large_eta_seconds;
                overload_input.queued_small_files = queued_small;
                overload_input.small_low_watermark_files = file_queues.small.resume_entries;
                overload_input.small_high_watermark_files = file_queues.small.max_entries;
                overload_input.small_low_watermark_intervals = small_low_watermark_intervals;
                overload_input.small_scanner_sleep_ratio =
                    queued_small >= file_queues.small.max_entries ? 1.0 : 0.0;
                overload_input.queued_large_files = queued_large;
                overload_input.large_queue_capacity_files = file_queues.large.max_entries;
                overload_input.total_gigabits_per_second = total_byte_rate * 8.0 / 1'000'000'000.0;
                overload_input.large_gigabits_per_second = large_byte_rate * 8.0 / 1'000'000'000.0;
                const BucketPathOverloadScores overload_scores =
                    evaluate_bucket_path_overload_impl(overload_input);

                AutoScaleMetrics small_lane_metrics;
                small_lane_metrics.input_fullness =
                    static_cast<double>(queued_small) /
                    static_cast<double>(std::max<std::size_t>(1, file_queues.small.max_entries));
                small_lane_metrics.input_available_ratio = small_lane_metrics.input_fullness;
                small_lane_metrics.busy_ratio = queued_small == 0U ? 0.0 : 1.0;
                small_lane_metrics.throughput_per_second = small_rate;
                small_lane_metrics.overload_score = overload_scores.small_score;

                AutoScaleMetrics large_lane_metrics;
                large_lane_metrics.input_fullness =
                    static_cast<double>(queued_large) /
                    static_cast<double>(std::max<std::size_t>(1, file_queues.large.max_entries));
                large_lane_metrics.input_available_ratio = large_lane_metrics.input_fullness;
                large_lane_metrics.busy_ratio = queued_large == 0U ? 0.0 : 1.0;
                large_lane_metrics.throughput_per_second = large_byte_rate;
                large_lane_metrics.overload_score = overload_scores.large_score;

                const AutoScaleDecision small_lane_decision =
                    small_lane_scaler.update(small_lane_metrics);
                const AutoScaleDecision large_lane_decision =
                    large_lane_scaler.update(large_lane_metrics);
                const std::size_t applied_small =
                    small_reader_job.set_active_worker_limit(small_lane_decision.active_workers);
                const std::size_t applied_large =
                    large_reader_job.set_active_worker_limit(large_lane_decision.active_workers);
                large_reader_small_priority_percent.store(
                    decision.large_reader_small_priority_percent,
                    std::memory_order_relaxed);
                const SplitScannerCapacityDecision scanner_decision =
                    choose_split_scanner_capacity_impl(small_metadata_threads,
                                                       large_metadata_threads,
                                                       large_scanner_floor,
                                                       decision.large_reader_small_priority_percent);
                const std::size_t small_scanners =
                    set_scanner_active_workers(small_scanner_capacity,
                                               scanner_decision.small_scanners,
                                               small_metadata_worker_slots);
                const std::size_t large_scanners =
                    set_scanner_active_workers(large_scanner_capacity,
                                               scanner_decision.large_scanners,
                                               large_metadata_worker_slots);
                std::size_t recon_scanners = 0;
                if (recon_scan_enabled) {
                    const bool recon_startup =
                        !snapshot.recon_completed &&
                        (now - coordinator_started_at < recon_startup_window ||
                         snapshot.recon_small_files_found < 100'000'000U);
                    recon_scanners = set_scanner_active_workers(
                        recon_scanner_capacity,
                        recon_startup ? recon_metadata_threads : recon_steady_metadata_threads,
                        recon_metadata_threads);
                }
                std::cerr << "bucket_priority small_workers=" << applied_small
                          << " large_workers=" << applied_large
                          << " small_scanners=" << small_scanners
                          << " large_scanners=" << large_scanners
                          << " recon_scanners=" << recon_scanners
                          << " recon_scanner_max=" << recon_metadata_threads
                          << " recon_scanner_steady=" << recon_steady_metadata_threads
                          << " large_reader_small_priority_percent="
                          << decision.large_reader_small_priority_percent
                          << " small_overload_score=" << overload_scores.small_score
                          << " large_overload_score=" << overload_scores.large_score
                          << " small_autoscale_reason=" << small_lane_decision.reason
                          << " large_autoscale_reason=" << large_lane_decision.reason
                          << " small_eta_seconds=" << decision.small_eta_seconds
                          << " large_eta_seconds=" << decision.large_eta_seconds
                          << " small_rate_files_per_second=" << small_rate
                          << " large_rate_files_per_second=" << large_rate
                          << " large_rate_bytes_per_second=" << large_byte_rate
                          << " small_remaining="
                          << (small_total > snapshot.small_files_read
                                  ? small_total - snapshot.small_files_read
                                  : 0U)
                          << " large_remaining="
                          << (large_total > snapshot.large_files_read
                                  ? large_total - snapshot.large_files_read
                                  : 0U)
                          << " large_remaining_bytes="
                          << (large_total_bytes > snapshot.large_bytes_read
                                  ? large_total_bytes - snapshot.large_bytes_read
                                  : 0U)
                          << " large_total_bytes=" << large_total_bytes
                          << " total_source=" << displayed_totals.source
                          << " displayed_small_total=" << displayed_totals.small_total
                          << " displayed_large_total=" << displayed_totals.large_total
                          << " displayed_large_total_bytes=" << displayed_totals.large_total_bytes
                          << " control_small_total=" << small_total
                          << " control_large_total=" << large_total
                          << " control_large_total_bytes=" << large_total_bytes
                          << " production_small_found=" << snapshot.small_files_found
                          << " production_large_found=" << snapshot.large_files_found
                          << " production_large_logical_size_bytes=" << snapshot.large_logical_size_bytes
                          << " recon_small_found=" << snapshot.recon_small_files_found
                          << " recon_large_found=" << snapshot.recon_large_files_found
                          << " recon_large_logical_size_bytes=" << snapshot.recon_large_logical_size_bytes
                          << " queued_small_files=" << queued_small
                          << " queued_large_files=" << queued_large
                          << " scan_completed="
                          << (production_scan_completed.load(std::memory_order_relaxed) ? "true" : "false")
                          << " recon_completed=" << (snapshot.recon_completed ? "true" : "false")
                          << '\n';

                if (last_human_report == std::chrono::steady_clock::time_point{} ||
                    now - last_human_report >= std::chrono::minutes(1)) {
                    last_human_report = now;
                    const double small_percent =
                        small_total == 0U ? 0.0 : percentage_of(snapshot.small_files_read, small_total);
                    const double large_percent =
                        large_total_bytes == 0U ? 0.0 : percentage_of(snapshot.large_bytes_read,
                                                                      large_total_bytes);
                    std::cerr << "progress "
                              << compact_clock_time_string(std::chrono::system_clock::now())
                              << " , s: " << human_count(static_cast<double>(displayed_totals.small_total))
                              << "/" << percent_string(small_percent)
                              << " " << human_count_rate(small_rate)
                              << " eta:" << compact_eta_duration(decision.small_eta_seconds)
                              << " , L: " << human_capacity(displayed_totals.large_total_bytes)
                              << "(" << human_count(static_cast<double>(displayed_totals.large_total))
                              << " files)"
                              << "/" << percent_string(large_percent)
                              << " " << human_gbit_rate(large_byte_rate)
                              << " eta:" << compact_eta_duration(decision.large_eta_seconds)
                              << " , T: " << human_gbit_rate(total_byte_rate)
                              << " , ms:" << small_scanners << '/' << large_scanners
                              << " , rs:" << recon_scanners << '/' << recon_metadata_threads
                              << " , src:" << displayed_totals.source
                              << " , ctrl:" << human_count(static_cast<double>(small_total))
                              << "/" << human_capacity(large_total_bytes)
                              << " , scan:"
                              << (production_scan_completed.load(std::memory_order_relaxed) ? "done" : "run")
                              << " recon:" << (snapshot.recon_completed ? "done" : "run")
                              << (snapshot.recon_completed ? " full" : " so_far")
                              << '\n';
                }

                for (int tick = 0; tick < 50 &&
                                   !bucket_priority_stop.load(std::memory_order_relaxed);
                     ++tick) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
            }
        });
    }

    StatusRegistry status_registry;
    std::unique_ptr<StatusServer> status_server;
    if (!status_socket_path.empty()) {
        status_registry.register_queue("small_file_work_queue", [&file_queues]() {
            return monitor_data_file_queue("small_file_work_queue", file_queues.small);
        });
        status_registry.register_queue("large_file_work_queue", [&file_queues]() {
            return monitor_data_file_queue("large_file_work_queue", file_queues.large);
        });
        if (dual_scan_small_large) {
            status_registry.register_queue("small_folder_work_queue", [&small_folder_queue]() {
                return monitor_flat_folder_queue("small_folder_work_queue", small_folder_queue);
            });
            status_registry.register_queue("large_folder_work_queue", [&large_folder_queue]() {
                return monitor_flat_folder_queue("large_folder_work_queue", large_folder_queue);
            });
        } else {
            status_registry.register_queue("folder_work_queue", [&folder_queue]() {
                return monitor_flat_folder_queue("folder_work_queue", folder_queue);
            });
        }
        if (recon_scan_enabled) {
            status_registry.register_queue("recon_folder_work_queue", [&recon_folder_queue]() {
                return monitor_flat_folder_queue("recon_folder_work_queue", recon_folder_queue);
            });
        }
        status_registry.register_queue("small_reader_output", [&small_reader_to_discard]() {
            return monitor_buf_queue("small_reader_output", small_reader_to_discard);
        });
        status_registry.register_queue("large_reader_output", [&large_reader_to_discard]() {
            return monitor_buf_queue("large_reader_output", large_reader_to_discard);
        });
        status_server = std::make_unique<StatusServer>(status_socket_path, status_registry);
        status_server->start();
    }

    const auto benchmark_started_at = std::chrono::steady_clock::now();
    stats.started_at = benchmark_started_at;
    stats.last_print_at = benchmark_started_at;
    if (max_duration_seconds > 0.0) {
        const auto stop_at = benchmark_started_at +
                             std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                 std::chrono::duration<double>(max_duration_seconds));
        folder_queue.stop_at = stop_at;
        small_folder_queue.stop_at = stop_at;
        large_folder_queue.stop_at = stop_at;
        recon_folder_queue.stop_at = stop_at;
        file_queues.small.stop_at = stop_at;
        file_queues.large.stop_at = stop_at;
    }

    std::mutex stop_timer_mutex;
    std::condition_variable stop_timer_cv;
    bool cancel_stop_timer = false;
    std::thread stop_timer;
    if (folder_queue.stop_at.has_value()) {
        const auto stop_at = *folder_queue.stop_at;
        stop_timer = std::thread([&]() {
            std::unique_lock<std::mutex> lock(stop_timer_mutex);
            const bool cancelled = stop_timer_cv.wait_until(lock, stop_at, [&]() {
                return cancel_stop_timer;
            });
            if (!cancelled) {
                request_flat_folder_stop(folder_queue);
                request_flat_folder_stop(small_folder_queue);
                request_flat_folder_stop(large_folder_queue);
                request_flat_folder_stop(recon_folder_queue);
                request_data_file_stop(file_queues.small);
                request_data_file_stop(file_queues.large);
                small_reader_job.stop();
                large_reader_job.stop();
            }
        });
    }

    std::thread stats_printer([&]() {
        const auto interval = std::chrono::seconds(stats.print_interval_seconds);
        std::unique_lock<std::mutex> lock(stats.printer_mutex);
        while (true) {
            if (stats.printer_cv.wait_for(lock, interval, [&stats] {
                    return stats.printer_done.load(std::memory_order_relaxed);
                })) {
                break;
            }
            std::lock_guard<std::mutex> print_lock(stats.print_mutex);
            stats.last_print_at = std::chrono::steady_clock::now();
            const DataReadBenchmarkSnapshot snapshot =
                snapshot_data_read_stats(stats, recon_scan_enabled ? &recon_stats : nullptr);
            std::cerr << "split_data_read_stats gigabits_per_second=" << snapshot.gigabits_per_second
                      << " small_gigabits_per_second=" << snapshot.small_gigabits_per_second
                      << " large_gigabits_per_second=" << snapshot.large_gigabits_per_second
                      << " files_per_second=" << snapshot.files_per_second
                      << " small_files_per_second=" << snapshot.small_files_per_second
                      << " large_files_per_second=" << snapshot.large_files_per_second
                      << " bytes_read=" << snapshot.bytes_read
                      << " small_bytes_read=" << snapshot.small_bytes_read
                      << " large_bytes_read=" << snapshot.large_bytes_read
                      << " small_logical_size_bytes=" << snapshot.small_logical_size_bytes
                      << " large_logical_size_bytes=" << snapshot.large_logical_size_bytes
                      << " files_read=" << snapshot.files_read
                      << " small_files_read=" << snapshot.small_files_read
                      << " large_files_read=" << snapshot.large_files_read
                      << " small_files_found=" << snapshot.small_files_found
                      << " large_files_found=" << snapshot.large_files_found
                      << " recon_files_found=" << snapshot.recon_files_found
                      << " recon_small_files_found=" << snapshot.recon_small_files_found
                      << " recon_large_files_found=" << snapshot.recon_large_files_found
                      << " recon_small_logical_size_bytes=" << snapshot.recon_small_logical_size_bytes
                      << " recon_large_logical_size_bytes=" << snapshot.recon_large_logical_size_bytes
                      << " recon_completed=" << (snapshot.recon_completed ? "true" : "false")
                      << " queued_small_files=" << queued_data_read_files(file_queues.small)
                      << " queued_large_files=" << queued_data_read_files(file_queues.large)
                      << " small_reader_workers=" << small_reader_job.active_worker_limit()
                      << " large_reader_workers=" << large_reader_job.active_worker_limit()
                      << " large_reader_small_priority_percent="
                      << large_reader_small_priority_percent.load(std::memory_order_relaxed)
                      << " small_output_depth=" << small_reader_to_discard.size()
                      << " large_output_depth=" << large_reader_to_discard.size()
                      << " elapsed_seconds=" << snapshot.elapsed_seconds << '\n';
        }
    });

    small_discarder.start();
    large_discarder.start();
    large_reader_job.start();
    small_reader_job.start();
    if (large_autoscaler) {
        large_autoscaler->start();
    }
    if (pipeline_autoscaler) {
        pipeline_autoscaler->start();
    }

    std::vector<std::thread> metadata_workers;
    std::vector<std::thread> recon_workers;
    if (dual_scan_small_large) {
        metadata_workers.reserve(small_metadata_worker_slots + large_metadata_worker_slots);
        for (std::size_t index = 0; index < small_metadata_worker_slots; ++index) {
            metadata_workers.emplace_back(scan_filtered_split_data_read_metadata_worker,
                                          meta_config.source_root,
                                          meta_config.recursive,
                                          std::max<std::size_t>(1, meta_config.async_directory_depth),
                                          meta_config.readdirplus_page_bytes,
                                          std::ref(small_folder_queue),
                                          std::ref(file_queues.small),
                                          file_queues.small_file_threshold,
                                          SplitDataReadRoute::Small,
                                          std::ref(stats),
                                          &small_scanner_capacity,
                                          index);
        }
        for (std::size_t index = 0; index < large_metadata_worker_slots; ++index) {
            metadata_workers.emplace_back(scan_filtered_split_data_read_metadata_worker,
                                          meta_config.source_root,
                                          meta_config.recursive,
                                          std::max<std::size_t>(1, meta_config.async_directory_depth),
                                          meta_config.readdirplus_page_bytes,
                                          std::ref(large_folder_queue),
                                          std::ref(file_queues.large),
                                          file_queues.small_file_threshold,
                                          SplitDataReadRoute::Large,
                                          std::ref(stats),
                                          &large_scanner_capacity,
                                          index);
        }
        if (recon_scan_enabled) {
            recon_workers.reserve(recon_metadata_threads);
            for (std::size_t index = 0; index < recon_metadata_threads; ++index) {
                recon_workers.emplace_back(scan_recon_metadata_worker,
                                           meta_config.source_root,
                                           meta_config.recursive,
                                           recon_async_depth,
                                           meta_config.readdirplus_page_bytes,
                                           std::ref(recon_folder_queue),
                                           std::ref(recon_stats),
                                           file_queues.small_file_threshold,
                                           recon_page_sleep_us,
                                           &recon_scanner_capacity,
                                           index);
            }
        }
    } else {
        metadata_workers.reserve(metadata_threads);
        for (std::size_t index = 0; index < metadata_threads; ++index) {
            metadata_workers.emplace_back(scan_split_data_read_metadata_worker,
                                          meta_config.source_root,
                                          meta_config.recursive,
                                          std::max<std::size_t>(1, meta_config.async_directory_depth),
                                          meta_config.readdirplus_page_bytes,
                                          std::ref(folder_queue),
                                          std::ref(file_queues),
                                          std::ref(stats));
        }
    }

    for (auto& worker : metadata_workers) {
        worker.join();
    }
    production_scan_completed.store(true, std::memory_order_relaxed);
    mark_data_file_input_done(file_queues.small);
    mark_data_file_input_done(file_queues.large);

    for (auto& worker : recon_workers) {
        worker.join();
    }

    if (large_autoscaler) {
        large_autoscaler->stop();
    }
    if (pipeline_autoscaler) {
        pipeline_autoscaler->stop();
    }
    bucket_priority_stop.store(true, std::memory_order_relaxed);
    if (bucket_priority_coordinator.joinable()) {
        bucket_priority_coordinator.join();
    }
    if (autoscale_profile_store) {
        autoscale_profile_store->update_learned_workers(autoscale_config.profile_name,
                                                        "small_data_reader",
                                                        small_reader_job.active_worker_limit());
        autoscale_profile_store->update_learned_workers(autoscale_config.profile_name,
                                                        "large_data_reader",
                                                        large_reader_job.active_worker_limit());
        autoscale_profile_store->save();
    }
    small_reader_job.set_active_worker_limit(small_reader_job.worker_count());
    large_reader_job.set_active_worker_limit(large_reader_job.worker_count());
    small_reader_job.wait();
    large_reader_job.wait();
    small_discarder.wait();
    large_discarder.wait();

    stats.printer_done.store(true, std::memory_order_relaxed);
    stats.printer_cv.notify_all();
    if (stats_printer.joinable()) {
        stats_printer.join();
    }

    {
        std::lock_guard<std::mutex> lock(stop_timer_mutex);
        cancel_stop_timer = true;
    }
    stop_timer_cv.notify_all();
    if (stop_timer.joinable()) {
        stop_timer.join();
    }
    if (status_server) {
        status_server->stop();
    }

    if (folder_queue.error) {
        std::rethrow_exception(folder_queue.error);
    }
    if (small_folder_queue.error) {
        std::rethrow_exception(small_folder_queue.error);
    }
    if (large_folder_queue.error) {
        std::rethrow_exception(large_folder_queue.error);
    }
    if (recon_folder_queue.error) {
        std::rethrow_exception(recon_folder_queue.error);
    }
    if (file_queues.small.error) {
        std::rethrow_exception(file_queues.small.error);
    }
    if (file_queues.large.error) {
        std::rethrow_exception(file_queues.large.error);
    }

    DataReadBenchmarkSnapshot snapshot =
        snapshot_data_read_stats(stats, recon_scan_enabled ? &recon_stats : nullptr);
    snapshot.data_buffer_slots = pool_slots;
    snapshot.data_queue_depth = queue_depth;
    return snapshot;
}

DataHashBenchmarkReport run_parallel_data_hash_scan(const NfsMetaReaderConfig& meta_config,
                                                    const NfsDataReaderConfig& data_config,
                                                    ContentHashAlgorithm algorithm,
                                                    std::size_t hash_worker_threads,
                                                    std::size_t hash_work_factor,
                                                    std::size_t max_files_queued,
                                                    std::size_t data_buffer_slots,
                                                    std::size_t data_queue_depth,
                                                    double max_duration_seconds,
                                                    std::uint32_t stats_interval_seconds,
                                                    const std::filesystem::path& status_socket_path) {
    FlatMetadataWorkQueue folder_queue;
    folder_queue.folders.push_back(FileSpec{});
    DataReadFileQueue file_queue;
    file_queue.max_entries = std::max<std::size_t>(1, max_files_queued);

    const std::size_t data_threads = std::max<std::size_t>(1, data_config.data_reader_worker_count);
    const std::size_t outstanding = std::max<std::size_t>(1, data_config.outstanding_requests);
    const std::size_t hash_threads = std::max<std::size_t>(1, hash_worker_threads);
    const std::size_t metadata_threads = std::max<std::size_t>(1, meta_config.worker_count);
    const std::size_t effective_hash_work_factor = std::max<std::size_t>(1, hash_work_factor);
    const std::size_t queue_depth =
        std::max<std::size_t>(1, data_queue_depth == 0 ? data_threads * outstanding * 2U : data_queue_depth);
    const std::size_t pool_slots =
        std::max<std::size_t>(data_threads * outstanding + queue_depth + hash_threads + 1U,
                              data_buffer_slots == 0 ? data_threads * outstanding * 3U + hash_threads + 1U
                                                     : data_buffer_slots);

    RawBufferPool data_pool = make_data_buffer_pool(pool_slots);
    BufferPoolRegistry registry;
    registry.register_pool(data_pool);
    BufQueue reader_to_hasher(queue_depth);
    BufQueue hasher_to_discard(queue_depth);

    DataReadBenchmarkStats stats;
    stats.print_interval_seconds = std::max<std::uint32_t>(1, stats_interval_seconds);
    stats.folders_found.store(1, std::memory_order_relaxed);

    NfsDataBufferReaderJob data_reader_job(
        data_config,
        data_pool,
        reader_to_hasher,
        [&file_queue]() {
            thread_local std::deque<FileSpec> worker_file_batch;
            if (worker_file_batch.empty()) {
                std::vector<FileSpec> next_batch = take_data_file_work_batch(file_queue, 128);
                for (auto& file : next_batch) {
                    worker_file_batch.push_back(std::move(file));
                }
            }
            if (worker_file_batch.empty()) {
                return std::optional<FileSpec> {};
            }
            FileSpec file = std::move(worker_file_batch.front());
            worker_file_batch.pop_front();
            return std::optional<FileSpec> {std::move(file)};
        },
        [&file_queue]() {
            return data_read_timer_expired(file_queue);
        });
    data_reader_job.set_bytes_read_callback([&stats](std::uint64_t bytes_read) {
        record_data_read_bytes(stats, bytes_read);
    });
    data_reader_job.set_file_read_callback([&stats]() {
        record_data_read_file(stats);
    });
    data_reader_job.set_file_failed_callback([&stats](const FileSpec&) {
        stats.files_failed.fetch_add(1, std::memory_order_relaxed);
    });

    DataHasherJob hasher(DataHasherConfig(hash_threads, algorithm, effective_hash_work_factor),
                         reader_to_hasher,
                         hasher_to_discard,
                         registry);
    BufferDiscarderJob discarder(BufferDiscarderConfig(1), hasher_to_discard, registry);

    StatusRegistry status_registry;
    std::unique_ptr<StatusServer> status_server;
    if (!status_socket_path.empty()) {
        const std::string meta_reader_job_name = backend_job_name("MetaReader", data_config.source_root);
        const std::string data_reader_job_name = backend_job_name("DataReader", data_config.source_root);
        status_registry.register_job(meta_reader_job_name, [&stats, metadata_threads, meta_reader_job_name]() {
            const DataReadBenchmarkSnapshot stats_snapshot = snapshot_data_read_stats(stats);
            MonitorJobSnapshot snapshot;
            snapshot.name = meta_reader_job_name;
            snapshot.running = true;
            snapshot.worker_count = metadata_threads;
            snapshot.processed_count = stats_snapshot.files_found + stats_snapshot.folders_found;
            snapshot.byte_count = stats_snapshot.logical_size_bytes;
            snapshot.count_unit = "records";
            snapshot.detail = "files_found=" + std::to_string(stats_snapshot.files_found) +
                              " folders_found=" + std::to_string(stats_snapshot.folders_found);
            return snapshot;
        });
        status_registry.register_job(data_reader_job_name, [&data_reader_job, data_reader_job_name]() {
            const NfsDataBufferReaderStats stats_snapshot = data_reader_job.stats();
            MonitorJobSnapshot snapshot;
            snapshot.name = data_reader_job_name;
            snapshot.running = stats_snapshot.running;
            snapshot.worker_count = stats_snapshot.worker_count;
            snapshot.processed_count = stats_snapshot.buffers_read;
            snapshot.byte_count = stats_snapshot.bytes_read;
            snapshot.count_unit = "buffers";
            snapshot.has_runtime_metrics = true;
            snapshot.runtime_metrics = data_reader_job.runtime_metrics().snapshot();
            snapshot.detail = "files_read=" + std::to_string(stats_snapshot.files_read) +
                              " files_failed=" + std::to_string(stats_snapshot.files_failed);
            return snapshot;
        });
        status_registry.register_job("data_hasher", [&hasher]() {
            const DataHasherStats stats_snapshot = hasher.stats();
            MonitorJobSnapshot snapshot;
            snapshot.name = "data_hasher";
            snapshot.running = stats_snapshot.running;
            snapshot.worker_count = stats_snapshot.worker_count;
            snapshot.processed_count = stats_snapshot.buffers_hashed;
            snapshot.byte_count = stats_snapshot.bytes_hashed;
            snapshot.count_unit = "buffers";
            snapshot.has_runtime_metrics = true;
            snapshot.runtime_metrics = hasher.runtime_metrics().snapshot();
            snapshot.detail = "hash=" + stats_snapshot.algorithm +
                              " work_factor=" + std::to_string(stats_snapshot.work_factor);
            return snapshot;
        });
        status_registry.register_job("buffer_discarder", [&discarder]() {
            const BufferDiscarderStats stats_snapshot = discarder.stats();
            MonitorJobSnapshot snapshot;
            snapshot.name = "buffer_discarder";
            snapshot.running = stats_snapshot.running;
            snapshot.worker_count = stats_snapshot.worker_count;
            snapshot.processed_count = stats_snapshot.buffers_discarded;
            snapshot.byte_count = stats_snapshot.bytes_discarded;
            snapshot.count_unit = "buffers";
            snapshot.has_runtime_metrics = true;
            snapshot.runtime_metrics = discarder.runtime_metrics().snapshot();
            return snapshot;
        });
        status_registry.register_queue("folder_work_queue", [&folder_queue]() {
            return monitor_flat_folder_queue("folder_work_queue", folder_queue);
        });
        status_registry.register_queue("file_work_queue", [&file_queue]() {
            return monitor_data_file_queue("file_work_queue", file_queue);
        });
        status_registry.register_queue("reader_to_hasher", [&reader_to_hasher]() {
            return monitor_buf_queue("reader_to_hasher", reader_to_hasher);
        });
        status_registry.register_queue("hasher_to_discard", [&hasher_to_discard]() {
            return monitor_buf_queue("hasher_to_discard", hasher_to_discard);
        });
        status_server = std::make_unique<StatusServer>(status_socket_path, status_registry);
        status_server->start();
    }

    const auto benchmark_started_at = std::chrono::steady_clock::now();
    stats.started_at = benchmark_started_at;
    stats.last_print_at = benchmark_started_at;
    if (max_duration_seconds > 0.0) {
        const auto stop_at = benchmark_started_at +
                             std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                 std::chrono::duration<double>(max_duration_seconds));
        folder_queue.stop_at = stop_at;
        file_queue.stop_at = stop_at;
    }

    std::mutex stop_timer_mutex;
    std::condition_variable stop_timer_cv;
    bool cancel_stop_timer = false;
    std::thread stop_timer;
    if (folder_queue.stop_at.has_value()) {
        const auto stop_at = *folder_queue.stop_at;
        stop_timer = std::thread([&]() {
            std::unique_lock<std::mutex> lock(stop_timer_mutex);
            const bool cancelled = stop_timer_cv.wait_until(lock, stop_at, [&]() {
                return cancel_stop_timer;
            });
            if (!cancelled) {
                request_flat_folder_stop(folder_queue);
                request_data_file_stop(file_queue);
                data_reader_job.stop();
            }
        });
    }

    std::thread stats_printer(run_data_hash_stats_printer,
                              std::ref(stats),
                              std::ref(file_queue),
                              std::cref(hasher),
                              std::cref(reader_to_hasher),
                              std::cref(hasher_to_discard));

    discarder.start();
    hasher.start();
    data_reader_job.start();

    std::vector<std::thread> metadata_workers;
    metadata_workers.reserve(metadata_threads);
    for (std::size_t index = 0; index < metadata_threads; ++index) {
        metadata_workers.emplace_back(scan_data_read_metadata_worker,
                                      meta_config.source_root,
                                      meta_config.recursive,
                                      std::max<std::size_t>(1, meta_config.async_directory_depth),
                                      meta_config.readdirplus_page_bytes,
                                      0,
                                      0,
                                      std::ref(folder_queue),
                                      std::ref(file_queue),
                                      std::ref(stats),
                                      nullptr,
                                      nullptr);
    }

    for (auto& worker : metadata_workers) {
        worker.join();
    }
    mark_data_file_input_done(file_queue);

    data_reader_job.wait();
    hasher.wait();
    discarder.wait();

    stats.printer_done.store(true, std::memory_order_relaxed);
    stats.printer_cv.notify_all();
    if (stats_printer.joinable()) {
        stats_printer.join();
    }

    {
        std::lock_guard<std::mutex> lock(stop_timer_mutex);
        cancel_stop_timer = true;
    }
    stop_timer_cv.notify_all();
    if (stop_timer.joinable()) {
        stop_timer.join();
    }

    if (status_server) {
        status_server->stop();
    }

    if (folder_queue.error) {
        std::rethrow_exception(folder_queue.error);
    }
    if (file_queue.error) {
        std::rethrow_exception(file_queue.error);
    }

    const DataReadBenchmarkSnapshot read = snapshot_data_read_stats(stats);
    const DataHasherStats hash = hasher.stats();

    DataHashBenchmarkReport report;
    report.files_found = read.files_found;
    report.folders_found = read.folders_found;
    report.files_read = read.files_read;
    report.files_failed = read.files_failed;
    report.logical_size_bytes = read.logical_size_bytes;
    report.bytes_read = read.bytes_read;
    report.bytes_hashed = hash.bytes_hashed;
    report.read_bytes_per_second = read.bytes_per_second;
    report.read_gigabits_per_second = read.gigabits_per_second;
    report.hash_bytes_per_second =
        read.elapsed_seconds > 0.0 ? static_cast<double>(hash.bytes_hashed) / read.elapsed_seconds : 0.0;
    report.hash_gigabits_per_second = report.hash_bytes_per_second * 8.0 / 1'000'000'000.0;
    report.elapsed_seconds = read.elapsed_seconds;
    report.data_buffer_slots = pool_slots;
    report.data_queue_depth = queue_depth;
    report.hash_work_factor = hash.work_factor;
    report.hash_algorithm = to_string(algorithm);
    return report;
}

HashInventorySnapshot snapshot_hash_inventory_stats(const HashInventoryStats& stats) {
    const auto now = std::chrono::steady_clock::now();
    const double elapsed = stats.started_at == std::chrono::steady_clock::time_point{}
                               ? 0.0
                               : std::chrono::duration<double>(now - stats.started_at).count();
    const std::uint64_t read_elapsed_nanoseconds =
        stats.read_elapsed_nanoseconds.load(std::memory_order_relaxed);
    const double read_elapsed = read_elapsed_nanoseconds != 0
                                    ? static_cast<double>(read_elapsed_nanoseconds) / 1'000'000'000.0
                                    : elapsed;
    HashInventorySnapshot snapshot;
    snapshot.files_found = stats.files_found.load(std::memory_order_relaxed);
    snapshot.folders_found = stats.folders_found.load(std::memory_order_relaxed);
    snapshot.files_hashed = stats.files_hashed.load(std::memory_order_relaxed);
    snapshot.files_failed = stats.files_failed.load(std::memory_order_relaxed);
    snapshot.logical_size_bytes = stats.logical_size_bytes.load(std::memory_order_relaxed);
    snapshot.bytes_read = stats.bytes_read.load(std::memory_order_relaxed);
    snapshot.bytes_hashed = stats.bytes_hashed.load(std::memory_order_relaxed);
    snapshot.elapsed_seconds = elapsed;
    snapshot.read_elapsed_seconds = read_elapsed;
    snapshot.read_bytes_per_second =
        read_elapsed > 0.0 ? static_cast<double>(snapshot.bytes_read) / read_elapsed : 0.0;
    snapshot.read_gigabits_per_second = snapshot.read_bytes_per_second * 8.0 / 1'000'000'000.0;
    snapshot.bytes_per_second = elapsed > 0.0 ? static_cast<double>(snapshot.bytes_hashed) / elapsed : 0.0;
    return snapshot;
}

HashMode parse_hash_mode(const std::string& value) {
    if (value.empty() || value == "file" || value == "full-file" || value == "full") {
        return HashMode::file;
    }
    if (value == "blocks" || value == "block" || value == "block-list") {
        return HashMode::blocks;
    }
    throw std::invalid_argument("hash mode must be file or blocks");
}

std::string to_string(HashMode mode) {
    switch (mode) {
        case HashMode::file:
            return "file";
        case HashMode::blocks:
            return "blocks";
    }
    return "unknown";
}

void record_hash_read_bytes(HashInventoryStats& stats, std::uint64_t bytes_read) {
    stats.bytes_read.fetch_add(bytes_read, std::memory_order_relaxed);
    const auto elapsed = std::chrono::steady_clock::now() - stats.started_at;
    const std::uint64_t elapsed_nanoseconds =
        static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
    std::uint64_t current = stats.read_elapsed_nanoseconds.load(std::memory_order_relaxed);
    while (elapsed_nanoseconds > current &&
           !stats.read_elapsed_nanoseconds.compare_exchange_weak(current,
                                                                 elapsed_nanoseconds,
                                                                 std::memory_order_relaxed,
                                                                 std::memory_order_relaxed)) {
    }
}

void record_hash_metadata(HashInventoryStats& stats,
                          std::size_t files_found,
                          std::size_t folders_found,
                          std::uint64_t logical_size_bytes) {
    stats.files_found.fetch_add(files_found, std::memory_order_relaxed);
    stats.folders_found.fetch_add(folders_found, std::memory_order_relaxed);
    stats.logical_size_bytes.fetch_add(logical_size_bytes, std::memory_order_relaxed);
}

void record_hash_metadata_batch(bool recursive,
                                FlatMetadataWorkQueue& folder_queue,
                                DataReadFileQueue& file_queue,
                                HashInventoryStats& stats,
                                MetadataRecordWriter* record_writer,
                                std::mutex* record_writer_mutex,
                                FlatFolderScanBatch batch) {
    if (batch.failed) {
        std::cerr << "metadata scan skipped folder '"
                  << (batch.folder.rel_path.empty() ? "/" : batch.folder.rel_path)
                  << "': " << (batch.error.empty() ? "unknown error" : batch.error) << '\n';
        if (batch.folder.rel_path.empty()) {
            const std::string message =
                batch.error.empty() ? "failed to scan root metadata folder" : batch.error;
            const auto error = std::make_exception_ptr(std::runtime_error(message));
            fail_flat_folder_work(folder_queue, error);
            fail_data_file_work(file_queue, error);
            return;
        }
        finish_flat_folder_work(folder_queue);
        return;
    }

    std::uint64_t logical_size_bytes = 0;
    for (const auto& file : batch.files) {
        logical_size_bytes += file.declared_size != 0 ? file.declared_size : file.content.size();
    }

    std::vector<FileSpec> child_work;
    if (recursive && !flat_metadata_scan_should_stop(folder_queue)) {
        child_work.reserve(batch.directories.size());
        for (auto& directory : batch.directories) {
            directory.rel_path = normalize_path(directory.rel_path);
            child_work.push_back(directory);
        }
    }

    record_hash_metadata(stats, batch.files.size(), batch.directories.size(), logical_size_bytes);
    if (record_writer != nullptr) {
        MetadataFolderRecord folder_record;
        folder_record.spec = std::move(batch.folder);
        folder_record.flat_file_count = batch.files.size();
        folder_record.flat_logical_size_bytes = logical_size_bytes;
        std::unique_lock<std::mutex> writer_lock;
        if (record_writer_mutex != nullptr) {
            writer_lock = std::unique_lock<std::mutex>(*record_writer_mutex);
        }
        record_writer->write_batch({}, std::vector<MetadataFolderRecord>{std::move(folder_record)});
    }
    if (!enqueue_data_read_files(file_queue, std::move(batch.files))) {
        finish_flat_folder_work(folder_queue);
        return;
    }
    enqueue_flat_folder_work(folder_queue, std::move(child_work));
    finish_flat_folder_work(folder_queue);
}

void scan_hash_metadata_worker(const std::string& source_root,
                               bool recursive,
                               std::size_t async_directory_depth,
                               FlatMetadataWorkQueue& folder_queue,
                               DataReadFileQueue& file_queue,
                               HashInventoryStats& stats,
                               MetadataRecordWriter* record_writer,
                               std::mutex* record_writer_mutex) {
    auto backend = make_nfs_backend(source_root);
    try {
        backend->scan_flat_folders(
            async_directory_depth,
            [&folder_queue](bool wait_for_work) {
                return take_flat_folder_work(folder_queue, wait_for_work);
            },
            [&folder_queue, &file_queue] {
                return flat_metadata_scan_should_stop(folder_queue) || data_read_timer_expired(file_queue);
            },
            [recursive, &folder_queue, &file_queue, &stats, record_writer, record_writer_mutex](FlatFolderScanBatch batch) {
                record_hash_metadata_batch(recursive,
                                           folder_queue,
                                           file_queue,
                                           stats,
                                           record_writer,
                                           record_writer_mutex,
                                           std::move(batch));
            });
    } catch (...) {
        const std::exception_ptr error = std::current_exception();
        fail_flat_folder_work(folder_queue, error);
        fail_data_file_work(file_queue, error);
    }
}

std::size_t hash_chunk_message_bytes(const HashChunkMessage& message) {
    return static_cast<std::size_t>(message.data_len);
}

void fail_hash_chunk_queue(HashChunkShardQueue& queue, std::exception_ptr error = std::current_exception()) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.stop = true;
        if (!queue.error && error != nullptr) {
            queue.error = error;
        }
    }
    queue.cv_not_empty.notify_all();
    queue.cv_not_full.notify_all();
}

void fail_hash_chunk_queues(std::vector<std::unique_ptr<HashChunkShardQueue>>& queues,
                            std::exception_ptr error = std::current_exception()) {
    for (auto& queue : queues) {
        fail_hash_chunk_queue(*queue, error);
    }
}

bool enqueue_hash_chunk_message(HashChunkShardQueue& queue, HashChunkMessage&& message) {
    const std::size_t message_bytes = hash_chunk_message_bytes(message);
    std::unique_lock<std::mutex> lock(queue.mutex);
    queue.cv_not_full.wait(lock, [&queue]() {
        return queue.stop || queue.error || queue.messages.size() < queue.max_entries;
    });
    if (queue.stop || queue.error) {
        queue.cv_not_empty.notify_all();
        queue.cv_not_full.notify_all();
        return false;
    }

    queue.queued_bytes += message_bytes;
    queue.messages.push_back(std::move(message));
    lock.unlock();
    queue.cv_not_empty.notify_one();
    return true;
}

std::optional<HashChunkMessage> take_hash_chunk_message(HashChunkShardQueue& queue) {
    std::unique_lock<std::mutex> lock(queue.mutex);
    queue.cv_not_empty.wait(lock, [&queue]() {
        return queue.stop || queue.error || !queue.messages.empty() || queue.input_done;
    });
    if (queue.messages.empty()) {
        return std::nullopt;
    }

    HashChunkMessage message = std::move(queue.messages.front());
    queue.messages.pop_front();
    queue.queued_bytes -= hash_chunk_message_bytes(message);
    lock.unlock();
    queue.cv_not_full.notify_one();
    return message;
}

void mark_hash_chunk_input_done(std::vector<std::unique_ptr<HashChunkShardQueue>>& queues) {
    for (auto& queue : queues) {
        {
            std::lock_guard<std::mutex> lock(queue->mutex);
            queue->input_done = true;
        }
        queue->cv_not_empty.notify_all();
        queue->cv_not_full.notify_all();
    }
}

HashChunkShardQueue& hash_queue_for_file(std::vector<std::unique_ptr<HashChunkShardQueue>>& queues,
                                         std::uint64_t file_id) {
    return *queues[static_cast<std::size_t>(file_id % queues.size())];
}

std::string join_block_hashes(const std::vector<std::string>& block_hashes) {
    std::string joined;
    for (const auto& hash : block_hashes) {
        if (!joined.empty()) {
            joined.push_back(';');
        }
        joined += hash;
    }
    return joined;
}

std::uint64_t fold_digest_prefix(const std::uint8_t* digest, std::size_t digest_size) {
    std::uint64_t folded = 0;
    const std::size_t bytes = std::min<std::size_t>(sizeof(folded), digest_size);
    std::memcpy(&folded, digest, bytes);
    return folded;
}

std::uint64_t benchmark_hash_once(ContentHashAlgorithm algorithm, std::string_view data) {
    switch (algorithm) {
        case ContentHashAlgorithm::md5: {
            Md5State hasher;
            hasher.update(data);
            const auto digest = hasher.digest();
            return fold_digest_prefix(digest.data(), digest.size());
        }
        case ContentHashAlgorithm::sha256: {
            Sha256State hasher;
            hasher.update(data);
            const auto digest = hasher.digest();
            return fold_digest_prefix(digest.data(), digest.size());
        }
        case ContentHashAlgorithm::xxh64: {
            Hash64State hasher;
            hasher.update(data);
            return hasher.value();
        }
        case ContentHashAlgorithm::xxh3_64: {
            Xxh3_64State hasher;
            hasher.update(data);
            return hasher.value();
        }
        case ContentHashAlgorithm::xxh3_128: {
            Xxh3_128State hasher;
            hasher.update(data);
            const auto digest = hasher.digest();
            return fold_digest_prefix(digest.data(), digest.size());
        }
    }
    throw std::invalid_argument("unsupported hash algorithm");
}

std::string make_hash_benchmark_payload(std::size_t block_size) {
    std::string payload(block_size, '\0');
    std::uint64_t state = 0x9e3779b97f4a7c15ULL;
    for (char& ch : payload) {
        state ^= state >> 12U;
        state ^= state << 25U;
        state ^= state >> 27U;
        ch = static_cast<char>((state * 0x2545f4914f6cdd1dULL) & 0xffU);
    }
    return payload;
}

std::string active_hash_hex(ContentHashAlgorithm algorithm, const ActiveHashFile& active) {
    switch (algorithm) {
        case ContentHashAlgorithm::md5:
            return active.md5.hex_digest();
        case ContentHashAlgorithm::sha256:
            return active.sha256.hex_digest();
        case ContentHashAlgorithm::xxh64: {
            std::ostringstream out;
            out << std::hex << std::setfill('0') << std::setw(16) << active.xxh64.value();
            return out.str();
        }
        case ContentHashAlgorithm::xxh3_64:
            return active.xxh3_64.hex_digest();
        case ContentHashAlgorithm::xxh3_128:
            return active.xxh3_128.hex_digest();
    }
    throw std::invalid_argument("unsupported hash algorithm");
}

void reset_active_hash_state(ActiveHashFile& active) {
    active.md5 = Md5State {};
    active.sha256 = Sha256State {};
    active.xxh64 = Hash64State {};
    active.xxh3_64 = Xxh3_64State {};
    active.xxh3_128 = Xxh3_128State {};
}

void update_active_hash(ContentHashAlgorithm algorithm, ActiveHashFile& active, std::string_view data) {
    switch (algorithm) {
        case ContentHashAlgorithm::md5:
            active.md5.update(data);
            break;
        case ContentHashAlgorithm::sha256:
            active.sha256.update(data);
            break;
        case ContentHashAlgorithm::xxh64:
            active.xxh64.update(data);
            break;
        case ContentHashAlgorithm::xxh3_64:
            active.xxh3_64.update(data);
            break;
        case ContentHashAlgorithm::xxh3_128:
            active.xxh3_128.update(data);
            break;
    }
}

void update_active_block_hash(ContentHashAlgorithm algorithm,
                              ActiveHashFile& active,
                              HashInventoryStats& stats,
                              std::uint64_t hash_block_size,
                              std::string_view data) {
    const std::uint64_t block_size = std::max<std::uint64_t>(1, hash_block_size);
    std::string_view remaining = data;
    while (!remaining.empty()) {
        if (active.current_block_expected == 0) {
            const std::uint64_t block_start = active.next_block_index * block_size;
            const std::uint64_t remaining_file_bytes =
                active.file.declared_size > block_start ? active.file.declared_size - block_start : 0;
            active.current_block_expected = std::min<std::uint64_t>(block_size, remaining_file_bytes);
            active.current_block_bytes = 0;
            if (active.current_block_expected == 0) {
                throw std::runtime_error("block hash data exceeds declared file size");
            }
            reset_active_hash_state(active);
        }

        const std::size_t bytes_to_take = static_cast<std::size_t>(
            std::min<std::uint64_t>(active.current_block_expected - active.current_block_bytes, remaining.size()));
        const std::string_view segment = remaining.substr(0, bytes_to_take);
        update_active_hash(algorithm, active, segment);
        active.current_block_bytes += bytes_to_take;
        active.bytes_hashed += bytes_to_take;
        stats.bytes_hashed.fetch_add(bytes_to_take, std::memory_order_relaxed);

        if (active.current_block_bytes == active.current_block_expected) {
            active.block_hashes.push_back(active_hash_hex(algorithm, active));
            ++active.next_block_index;
            active.current_block_bytes = 0;
            active.current_block_expected = 0;
        }

        remaining.remove_prefix(bytes_to_take);
    }
}

void update_active_hash_from_payload(ContentHashAlgorithm algorithm,
                                     ActiveHashFile& active,
                                     HashInventoryStats& stats,
                                     HashMode hash_mode,
                                     std::uint64_t hash_block_size,
                                     std::string_view payload) {
    if (hash_mode == HashMode::blocks) {
        update_active_block_hash(algorithm, active, stats, hash_block_size, payload);
        return;
    }

    update_active_hash(algorithm, active, payload);
    active.bytes_hashed += payload.size();
    stats.bytes_hashed.fetch_add(payload.size(), std::memory_order_relaxed);
}

void release_pending_hash_slots(ActiveHashFile& active, DataSlotPool& hash_data_pool) {
    for (const auto& [offset, handle] : active.pending_data_slots) {
        (void)offset;
        hash_data_pool.release(handle);
    }
    active.pending_data_slots.clear();
}

void drain_ready_hash_data(ContentHashAlgorithm algorithm,
                           ActiveHashFile& active,
                           DataSlotPool& hash_data_pool,
                           HashInventoryStats& stats,
                           HashMode hash_mode,
                           std::uint64_t hash_block_size) {
    while (true) {
        const auto found = active.pending_data_slots.find(active.next_data_offset);
        if (found == active.pending_data_slots.end()) {
            return;
        }

        const DataSlotHandle handle = found->second;
        active.pending_data_slots.erase(found);

        const DataBufTrailer& trailer = hash_data_pool.trailer(handle);
        if (trailer.data_offset != active.next_data_offset) {
            hash_data_pool.release(handle);
            throw std::runtime_error("hash data slot offset does not match expected file offset");
        }

        const std::string_view payload = hash_data_pool.data_view(handle);
        const std::size_t payload_size = payload.size();
        try {
            update_active_hash_from_payload(algorithm, active, stats, hash_mode, hash_block_size, payload);
        } catch (...) {
            hash_data_pool.release(handle);
            throw;
        }
        active.next_data_offset += payload_size;
        hash_data_pool.release(handle);
    }
}

void hash_data_reader_worker(NfsDataReaderConfig reader_config,
                             DataReadFileQueue& file_queue,
                             FlatMetadataWorkQueue& folder_queue,
                             std::vector<std::unique_ptr<HashChunkShardQueue>>& hash_queues,
                             DataSlotPool& hash_data_pool,
                             HashInventoryStats& stats,
                             std::atomic<std::uint64_t>& next_file_id) {
    NfsDataReader reader(std::move(reader_config));
    try {
        while (auto file = take_data_file_work(file_queue)) {
            const std::uint64_t file_id = next_file_id.fetch_add(1, std::memory_order_relaxed);
            HashChunkShardQueue& hash_queue = hash_queue_for_file(hash_queues, file_id);
            HashChunkMessage begin;
            begin.kind = HashChunkKind::begin;
            begin.file_id = file_id;
            begin.file = *file;
            if (!enqueue_hash_chunk_message(hash_queue, std::move(begin))) {
                throw std::runtime_error("hash queue stopped before file read");
            }

            try {
                (void)reader.stream_file_pooled_chunks(*file, hash_data_pool, [&](PooledFileChunk&& chunk) {
                    HashChunkMessage data;
                    data.kind = HashChunkKind::data;
                    data.file_id = file_id;
                    data.offset = chunk.offset;
                    data.handle = chunk.handle;
                    data.data_len = hash_data_pool.trailer(chunk.handle).data_len;
                    data.has_data_slot = true;
                    const std::uint64_t chunk_size = data.data_len;
                    bool enqueued = false;
                    try {
                        enqueued = enqueue_hash_chunk_message(hash_queue, std::move(data));
                    } catch (...) {
                        hash_data_pool.release(chunk.handle);
                        throw;
                    }
                    if (!enqueued) {
                        hash_data_pool.release(chunk.handle);
                        throw std::runtime_error("hash queue stopped while streaming file data");
                    }
                    record_hash_read_bytes(stats, chunk_size);
                });
            } catch (...) {
                HashChunkMessage failed;
                failed.kind = HashChunkKind::failed;
                failed.file_id = file_id;
                if (!enqueue_hash_chunk_message(hash_queue, std::move(failed))) {
                    throw;
                }
                stats.files_failed.fetch_add(1, std::memory_order_relaxed);
                continue;
            }

            HashChunkMessage end;
            end.kind = HashChunkKind::end;
            end.file_id = file_id;
            if (!enqueue_hash_chunk_message(hash_queue, std::move(end))) {
                throw std::runtime_error("hash queue stopped before file finalization");
            }
        }
    } catch (...) {
        const std::exception_ptr error = std::current_exception();
        fail_data_file_work(file_queue, error);
        fail_flat_folder_work(folder_queue, error);
        fail_hash_chunk_queues(hash_queues, error);
    }
}

void hash_stage_worker(ContentHashAlgorithm algorithm,
                       HashChunkShardQueue& hash_queue,
                       DataSlotPool& hash_data_pool,
                       DataReadFileQueue& file_queue,
                       FlatMetadataWorkQueue& folder_queue,
                       HashInventoryStats& stats,
                       MetadataRecordWriter* record_writer,
                       std::mutex* record_writer_mutex,
                       HashMode hash_mode,
                       std::uint64_t hash_block_size) {
    const std::string algorithm_name = to_string(algorithm);
    std::unordered_map<std::uint64_t, ActiveHashFile> active_files;
    try {
        while (auto message = take_hash_chunk_message(hash_queue)) {
            if (message->kind == HashChunkKind::begin) {
                ActiveHashFile active;
                active.file = std::move(message->file);
                active_files.emplace(message->file_id, std::move(active));
                continue;
            }
            if (message->kind == HashChunkKind::failed) {
                if (const auto failed_file = active_files.find(message->file_id); failed_file != active_files.end()) {
                    release_pending_hash_slots(failed_file->second, hash_data_pool);
                }
                active_files.erase(message->file_id);
                continue;
            }

            auto found = active_files.find(message->file_id);
            if (found == active_files.end()) {
                throw std::runtime_error("hash chunk arrived for an unknown file");
            }
            ActiveHashFile& active = found->second;
            if (message->kind == HashChunkKind::data) {
                if (!message->has_data_slot) {
                    throw std::runtime_error("hash data chunk arrived without a data slot");
                }
                const DataBufTrailer& trailer = hash_data_pool.trailer(message->handle);
                if (message->offset != trailer.data_offset) {
                    hash_data_pool.release(message->handle);
                    throw std::runtime_error("hash data message offset does not match data slot offset");
                }
                if (message->offset < active.next_data_offset) {
                    hash_data_pool.release(message->handle);
                    throw std::runtime_error("hash data chunk arrived for an already hashed offset");
                }
                const auto [inserted_at, inserted] = active.pending_data_slots.emplace(message->offset, message->handle);
                if (!inserted) {
                    (void)inserted_at;
                    hash_data_pool.release(message->handle);
                    throw std::runtime_error("duplicate hash data chunk arrived for a file offset");
                }
                drain_ready_hash_data(algorithm, active, hash_data_pool, stats, hash_mode, hash_block_size);
                continue;
            }

            if (message->kind == HashChunkKind::end) {
                drain_ready_hash_data(algorithm, active, hash_data_pool, stats, hash_mode, hash_block_size);
                if (!active.pending_data_slots.empty()) {
                    release_pending_hash_slots(active, hash_data_pool);
                    throw std::runtime_error("file ended with out-of-order hash data still pending");
                }
                if (active.next_data_offset != active.file.declared_size) {
                    throw std::runtime_error("file ended before all data offsets were hashed");
                }
                if (hash_mode == HashMode::blocks) {
                    if (active.current_block_bytes != 0 || active.current_block_expected != 0) {
                        throw std::runtime_error("file ended with incomplete block hash state");
                    }
                    const std::uint64_t block_size = std::max<std::uint64_t>(1, hash_block_size);
                    const std::uint64_t block_count =
                        active.file.declared_size == 0 ? 0 : (active.file.declared_size + block_size - 1U) / block_size;
                    if (active.block_hashes.size() != static_cast<std::size_t>(block_count)) {
                        throw std::runtime_error("file ended with missing block hashes");
                    }
                    active.file.hash_algorithm.clear();
                    active.file.content_hash.clear();
                    active.file.hash_block_size = block_size;
                    active.file.hash_block_count = block_count;
                    active.file.block_hash_algorithm = algorithm_name;
                    active.file.block_hashes = join_block_hashes(active.block_hashes);
                } else {
                    active.file.hash_algorithm = algorithm_name;
                    active.file.content_hash = active_hash_hex(algorithm, active);
                }
                if (record_writer != nullptr) {
                    std::unique_lock<std::mutex> writer_lock;
                    if (record_writer_mutex != nullptr) {
                        writer_lock = std::unique_lock<std::mutex>(*record_writer_mutex);
                    }
                    record_writer->write_batch(std::vector<FileSpec>{std::move(active.file)}, {});
                }
                stats.files_hashed.fetch_add(1, std::memory_order_relaxed);
                active_files.erase(found);
            }
        }
    } catch (...) {
        for (auto& [file_id, active] : active_files) {
            (void)file_id;
            release_pending_hash_slots(active, hash_data_pool);
        }
        const std::exception_ptr error = std::current_exception();
        fail_hash_chunk_queue(hash_queue, error);
        fail_data_file_work(file_queue, error);
        fail_flat_folder_work(folder_queue, error);
    }
}

HashInventorySnapshot run_parallel_hash_inventory_scan(const NfsMetaReaderConfig& meta_config,
                                                       const NfsDataReaderConfig& data_config,
                                                       ContentHashAlgorithm algorithm,
                                                       HashMode hash_mode,
                                                       std::size_t hash_worker_threads,
                                                       std::size_t max_files_queued,
                                                       std::size_t max_hash_chunks_queued,
                                                       std::uint64_t hash_block_size,
                                                       MetadataRecordWriter* record_writer,
                                                       double max_duration_seconds) {
    FlatMetadataWorkQueue folder_queue;
    folder_queue.folders.push_back(FileSpec{});
    DataReadFileQueue file_queue;
    file_queue.max_entries = std::max<std::size_t>(1, max_files_queued);

    if (max_duration_seconds > 0.0) {
        const auto stop_at = std::chrono::steady_clock::now() +
                             std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                 std::chrono::duration<double>(max_duration_seconds));
        folder_queue.stop_at = stop_at;
        file_queue.stop_at = stop_at;
    }

    HashInventoryStats stats;
    stats.started_at = std::chrono::steady_clock::now();
    stats.folders_found.store(1, std::memory_order_relaxed);

    const std::size_t hash_threads = std::max<std::size_t>(1, hash_worker_threads);
    const std::size_t data_threads = std::max<std::size_t>(1, data_config.data_reader_worker_count);
    const std::size_t reader_in_flight_slots =
        data_threads * std::max<std::size_t>(1, data_config.outstanding_requests);
    const std::size_t queued_hash_slots = std::max<std::size_t>(1, max_hash_chunks_queued);
    const std::size_t hash_data_pool_slots =
        reader_in_flight_slots + queued_hash_slots + hash_threads + 1U;
    DataSlotPool hash_data_pool(0, hash_data_pool_slots);
    std::mutex record_writer_mutex;
    std::vector<std::unique_ptr<HashChunkShardQueue>> hash_queues;
    std::vector<std::thread> hash_workers;
    const std::size_t per_hash_queue_entries =
        std::max<std::size_t>(1,
                              (std::max<std::size_t>(1, max_hash_chunks_queued) + hash_threads - 1) /
                                  hash_threads);
    hash_queues.reserve(hash_threads);
    for (std::size_t index = 0; index < hash_threads; ++index) {
        auto queue = std::make_unique<HashChunkShardQueue>();
        queue->max_entries = per_hash_queue_entries;
        hash_queues.push_back(std::move(queue));
    }

    hash_workers.reserve(hash_threads);
    for (std::size_t index = 0; index < hash_threads; ++index) {
        hash_workers.emplace_back(hash_stage_worker,
                                  algorithm,
                                  std::ref(*hash_queues[index]),
                                  std::ref(hash_data_pool),
                                  std::ref(file_queue),
                                  std::ref(folder_queue),
                                  std::ref(stats),
                                  record_writer,
                                  &record_writer_mutex,
                                  hash_mode,
                                  hash_block_size);
    }

    std::atomic<std::uint64_t> next_hash_file_id {1};
    std::vector<std::thread> data_workers;
    data_workers.reserve(data_threads);
    for (std::size_t index = 0; index < data_threads; ++index) {
        data_workers.emplace_back(hash_data_reader_worker,
                                  data_config,
                                  std::ref(file_queue),
                                  std::ref(folder_queue),
                                  std::ref(hash_queues),
                                  std::ref(hash_data_pool),
                                  std::ref(stats),
                                  std::ref(next_hash_file_id));
    }

    std::vector<std::thread> metadata_workers;
    const std::size_t metadata_threads = std::max<std::size_t>(1, meta_config.worker_count);
    metadata_workers.reserve(metadata_threads);
    for (std::size_t index = 0; index < metadata_threads; ++index) {
        metadata_workers.emplace_back(scan_hash_metadata_worker,
                                      meta_config.source_root,
                                      meta_config.recursive,
                                      std::max<std::size_t>(1, meta_config.async_directory_depth),
                                      std::ref(folder_queue),
                                      std::ref(file_queue),
                                      std::ref(stats),
                                      record_writer,
                                      &record_writer_mutex);
    }

    for (auto& worker : metadata_workers) {
        worker.join();
    }
    mark_data_file_input_done(file_queue);
    for (auto& worker : data_workers) {
        worker.join();
    }
    mark_hash_chunk_input_done(hash_queues);
    for (auto& worker : hash_workers) {
        worker.join();
    }

    if (folder_queue.error) {
        std::rethrow_exception(folder_queue.error);
    }
    if (file_queue.error) {
        std::rethrow_exception(file_queue.error);
    }
    for (auto& queue : hash_queues) {
        if (queue->error) {
            std::rethrow_exception(queue->error);
        }
    }

    return snapshot_hash_inventory_stats(stats);
}

bool receiver_queue_should_pause(std::uint64_t queued_bytes, std::uint64_t capacity_bytes) {
    const WatermarkStatus status = evaluate_watermarks(percentage_of(queued_bytes, capacity_bytes), 0.0);
    return status.soft_throttle || status.hard_stop;
}

bool receiver_queue_should_resume(std::uint64_t queued_bytes, std::uint64_t capacity_bytes) {
    return !receiver_queue_should_pause(queued_bytes, capacity_bytes);
}

void shutdown_receiver_sockets(const ReceiverSharedState& state) {
    if (state.priority_fd >= 0) {
        ::shutdown(state.priority_fd, SHUT_RDWR);
    }
    if (state.data_fd >= 0) {
        ::shutdown(state.data_fd, SHUT_RDWR);
    }
}

std::vector<FileSpec> scan_directories_for_transfer(const std::filesystem::path& source_root, bool recursive) {
    if (is_nfs_url(source_root.string())) {
        auto backend = make_nfs_backend(source_root.string());
        return backend->list_directories(recursive);
    }
    return collect_directory_specs(source_root, recursive);
}

void initialize_folder_report(TransferReport& report, const FileSpec& file) {
    const std::string folder_path = parent_path(file.rel_path);
    auto [it, inserted] = report.folders.emplace(folder_path, FolderRecord{});
    FolderRecord& folder = it->second;
    if (inserted) {
        folder.rel_path = folder_path;
        folder.parent_hash = folder_hash_for_path(parent_path(folder_path));
        folder.md_hash = path_hash(base_name(folder_path), folder.parent_hash);
        folder.state = FolderState::reading;
        folder.remote_state = FolderState::receiving;
    }
    ++folder.files_discovered;
    ++folder.files_total;
    folder.flat_size_bytes += file.declared_size;
}

void finalize_folder_report(TransferReport& report) {
    for (auto& [_, folder] : report.folders) {
        folder.state = FolderState::done;
        folder.remote_state = FolderState::done;
    }
}

void receiver_priority_loop(ReceiverSharedState& state) {
    auto target_backend = make_nfs_backend(state.runtime.target_root.string());
    auto target_writer = make_target_writer_backend(state.runtime.target_root.string());
    for (;;) {
        PriorityMessageType type = PriorityMessageType::session_end;
        std::string payload;
        if (!read_priority_payload(state.priority_fd, type, payload)) {
            break;
        }

        if (type == PriorityMessageType::session_start) {
            const SessionStartInfo info = decode_session_start(payload);
            state.session_large_chunk_bytes = info.large_chunk_bytes;
            state.skip_verify = info.skip_verify;
            continue;
        }
        if (type == PriorityMessageType::session_end) {
            std::lock_guard<std::mutex> lock(state.metadata_mutex);
            state.session_done = true;
            state.metadata_cv.notify_all();
            return;
        }
        if (type == PriorityMessageType::directory_record) {
            const FileSpec spec = make_directory_spec(decode_directory_record(payload));
            target_writer->ensure_directory(spec);
            state.directory_specs.push_back(spec);
            continue;
        }
        if (type != PriorityMessageType::file_record) {
            throw std::runtime_error("unexpected priority message type");
        }

        const FileRecordMessage record = decode_file_record(payload);
        const bool skip = target_matches_record(*target_backend, record);
        if (!skip) {
            std::lock_guard<std::mutex> lock(state.metadata_mutex);
            state.pending_records[record.file_id] = record;
            state.metadata_cv.notify_all();
        }

        const FileDecisionMessage decision{record.file_id, skip};
        std::lock_guard<std::mutex> write_lock(state.priority_write_mutex);
        send_file_decision(state.priority_fd, decision);
    }
}

bool receiver_has_all_pending_records(const ReceiverSharedState& state, const std::vector<std::uint64_t>& file_ids) {
    return std::all_of(file_ids.begin(), file_ids.end(), [&state](std::uint64_t file_id) {
        return state.pending_records.find(file_id) != state.pending_records.end();
    });
}

void send_receiver_file_ack(ReceiverSharedState& state,
                            const FileRecordMessage& record,
                            bool verified,
                            std::uint64_t bytes_written) {
    {
        std::lock_guard<std::mutex> write_lock(state.priority_write_mutex);
        send_file_ack(state.priority_fd, FileAckMessage{record.file_id, verified, bytes_written});
    }
    {
        std::lock_guard<std::mutex> lock(state.metadata_mutex);
        state.pending_records.erase(record.file_id);
    }
}

void process_packed_small_file_slot(ReceiverSharedState& state,
                                    TargetWriterBackend& target_writer,
                                    DataSlotPool& slot_pool,
                                    const DataSlotHandle& handle) {
    const auto entries = decode_packed_small_file_entries(slot_pool, handle);
    for (const auto& entry : entries) {
        FileRecordMessage record;
        {
            std::lock_guard<std::mutex> lock(state.metadata_mutex);
            const auto it = state.pending_records.find(entry.record.file_id);
            if (it == state.pending_records.end()) {
                throw std::runtime_error("packed small file arrived without matching file record");
            }
            record = it->second;
        }
        if (entry.data.size() != record.size) {
            throw std::runtime_error("packed small file size does not match file record");
        }

        FileSpec spec;
        spec.rel_path = record.rel_path;
        spec.mtime = record.mtime;
        spec.mode = record.mode;
        spec.uid = record.uid;
        spec.gid = record.gid;
        spec.declared_size = record.size;

        bool verified = false;
        try {
            target_writer.write_chunk(spec, entry.data, 0);
            target_writer.finish_file(spec);
            verified =
                state.skip_verify ||
                target_writer.file_hash(record.rel_path) == entry.data_hash;
        } catch (...) {
            target_writer.abort_file(record.rel_path);
        }

        send_receiver_file_ack(state,
                               record,
                               verified,
                               verified ? static_cast<std::uint64_t>(entry.data.size()) : 0U);
    }
}

void receiver_data_loop(ReceiverSharedState& state, int data_fd) {
    ReceiverChunkQueue queue;
    queue.capacity_bytes = receiver_queue_capacity_bytes(state);
    queue.queue.reset(std::max<std::size_t>(2,
                                            static_cast<std::size_t>(
                                                queue.capacity_bytes /
                                                    std::max<std::uint64_t>(1, state.session_large_chunk_bytes)) +
                                                2U));
    DataSlotPool slot_pool(state.small_pool_slots, state.large_pool_slots);

    std::thread writer_thread([&] {
        std::unordered_map<std::uint64_t, ReceiverFileContext> contexts;
        auto target_writer = make_target_writer_backend(state.runtime.target_root.string());

        try {
            for (;;) {
                ReceiverQueuedChunk work;
                bool should_exit = false;
                bool need_resume = false;
                {
                    std::unique_lock<std::mutex> lock(queue.mutex);
                    queue.cv_not_empty.wait(lock, [&queue] {
                        return queue.worker_error != nullptr || queue.input_done || !queue.queue.empty();
                    });
                    if (queue.worker_error != nullptr) {
                        return;
                    }
                    if (queue.queue.empty()) {
                        should_exit = queue.input_done;
                    } else {
                        work = queue.queue.pop();
                        queue.queued_bytes -= slot_pool.trailer(work.handle).data_len;
                        if (queue.paused && receiver_queue_should_resume(queue.queued_bytes, queue.capacity_bytes)) {
                            queue.paused = false;
                            need_resume = true;
                        }
                        queue.cv_not_full.notify_all();
                    }
                }

                if (need_resume) {
                    std::lock_guard<std::mutex> write_lock(state.priority_write_mutex);
                    send_resume(state.priority_fd);
                }
                if (should_exit) {
                    return;
                }

                if (is_packed_small_file_slot(slot_pool, work.handle)) {
                    try {
                        process_packed_small_file_slot(state, *target_writer, slot_pool, work.handle);
                    } catch (...) {
                        slot_pool.release(work.handle);
                        throw;
                    }
                    slot_pool.release(work.handle);
                    continue;
                }

                auto& context = contexts[slot_pool.trailer(work.handle).file_id];
                if (context.bytes_written == 0) {
                    context.record = work.record;
                }

                FileSpec spec;
                spec.rel_path = work.record.rel_path;
                spec.mtime = work.record.mtime;
                spec.mode = work.record.mode;
                spec.uid = work.record.uid;
                spec.gid = work.record.gid;
                spec.declared_size = work.record.size;

                try {
                    target_writer->write_chunk(spec,
                                               slot_pool.data_view(work.handle),
                                               slot_pool.trailer(work.handle).data_offset);
                } catch (...) {
                    target_writer->abort_file(work.record.rel_path);
                    send_receiver_file_ack(state, work.record, false, 0);
                    contexts.erase(slot_pool.trailer(work.handle).file_id);
                    slot_pool.release(work.handle);
                    continue;
                }
                context.bytes_written += slot_pool.trailer(work.handle).data_len;

                if ((slot_pool.trailer(work.handle).flags & kFlagLastChunk) == 0U) {
                    slot_pool.release(work.handle);
                    continue;
                }

                try {
                    target_writer->finish_file(spec);
                } catch (...) {
                    target_writer->abort_file(work.record.rel_path);
                    send_receiver_file_ack(state, work.record, false, 0);
                    contexts.erase(slot_pool.trailer(work.handle).file_id);
                    slot_pool.release(work.handle);
                    continue;
                }
                const bool verified =
                    state.skip_verify ||
                    target_writer->file_hash(work.record.rel_path) == slot_pool.trailer(work.handle).data_hash;
                send_receiver_file_ack(state, work.record, verified, verified ? context.bytes_written : 0U);
                contexts.erase(slot_pool.trailer(work.handle).file_id);
                slot_pool.release(work.handle);
            }
        } catch (...) {
            {
                std::lock_guard<std::mutex> lock(queue.mutex);
                queue.worker_error = std::current_exception();
                queue.cv_not_full.notify_all();
                queue.cv_not_empty.notify_all();
            }
            shutdown_receiver_sockets(state);
        }
    });

    try {
        for (;;) {
            DataSlotHandle handle;
            if (!read_data_slot(data_fd, slot_pool, handle)) {
                break;
            }

            FileRecordMessage record;
            {
                std::unique_lock<std::mutex> lock(state.metadata_mutex);
                if (is_packed_small_file_slot(slot_pool, handle)) {
                    const auto packed_ids = packed_small_file_ids(slot_pool, handle);
                    state.metadata_cv.wait(lock, [&state, &packed_ids] {
                        return state.session_done || receiver_has_all_pending_records(state, packed_ids);
                    });
                    if (!receiver_has_all_pending_records(state, packed_ids)) {
                        slot_pool.release(handle);
                        throw std::runtime_error("received packed data without matching file records");
                    }
                } else {
                    state.metadata_cv.wait(lock, [&state, &slot_pool, &handle] {
                        return state.session_done ||
                               state.pending_records.find(slot_pool.trailer(handle).file_id) !=
                                   state.pending_records.end();
                    });
                    const auto it = state.pending_records.find(slot_pool.trailer(handle).file_id);
                    if (it == state.pending_records.end()) {
                        slot_pool.release(handle);
                        throw std::runtime_error("received data chunk without matching file record");
                    }
                    record = it->second;
                }
            }

            const std::uint64_t chunk_bytes = slot_pool.trailer(handle).data_len;
            bool need_pause = false;
            {
                std::unique_lock<std::mutex> lock(queue.mutex);
                const std::uint64_t queue_limit = std::max(queue.capacity_bytes, chunk_bytes);
                queue.cv_not_full.wait(lock, [&queue, chunk_bytes, queue_limit] {
                    return queue.worker_error != nullptr || queue.queued_bytes + chunk_bytes <= queue_limit;
                });
                if (queue.worker_error != nullptr) {
                    slot_pool.release(handle);
                    std::rethrow_exception(queue.worker_error);
                }
                queue.queue.push(ReceiverQueuedChunk{handle, record});
                queue.queued_bytes += chunk_bytes;
                if (!queue.paused && receiver_queue_should_pause(queue.queued_bytes, queue.capacity_bytes)) {
                    queue.paused = true;
                    need_pause = true;
                }
                queue.cv_not_empty.notify_one();
            }

            if (need_pause) {
                std::lock_guard<std::mutex> write_lock(state.priority_write_mutex);
                send_pause(state.priority_fd);
            }
        }
    } catch (...) {
        {
            std::lock_guard<std::mutex> lock(queue.mutex);
            queue.input_done = true;
            queue.cv_not_empty.notify_all();
            queue.cv_not_full.notify_all();
        }
        writer_thread.join();
        throw;
    }

    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.input_done = true;
        queue.cv_not_empty.notify_all();
        queue.cv_not_full.notify_all();
    }
    writer_thread.join();

    if (queue.worker_error != nullptr) {
        std::rethrow_exception(queue.worker_error);
    }
}

bool poll_priority_message(int fd, int timeout_ms, PriorityMessageType& type, std::string& payload) {
    struct pollfd poll_state {
        fd, POLLIN, 0
    };
    const int ready = ::poll(&poll_state, 1, timeout_ms);
    if (ready < 0) {
        throw std::system_error(errno, std::generic_category(), "poll failed on priority socket");
    }
    if (ready == 0) {
        return false;
    }
    if ((poll_state.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
        throw std::runtime_error("priority socket closed unexpectedly");
    }
    return read_priority_payload(fd, type, payload);
}

void consume_sender_priority_message(PriorityMessageType type,
                                     std::string_view payload,
                                     bool& paused,
                                     std::deque<FileAckMessage>& pending_acks) {
    switch (type) {
        case PriorityMessageType::pause:
            paused = true;
            return;
        case PriorityMessageType::resume:
            paused = false;
            return;
        case PriorityMessageType::file_ack:
            pending_acks.push_back(decode_file_ack(payload));
            paused = false;
            return;
        default:
            throw std::runtime_error("unexpected priority message while sending data");
    }
}

void drain_sender_priority_events(int fd,
                                  int initial_timeout_ms,
                                  bool& paused,
                                  std::deque<FileAckMessage>& pending_acks) {
    PriorityMessageType type = PriorityMessageType::session_end;
    std::string payload;
    if (!poll_priority_message(fd, initial_timeout_ms, type, payload)) {
        return;
    }
    consume_sender_priority_message(type, payload, paused, pending_acks);
    while (poll_priority_message(fd, 0, type, payload)) {
        consume_sender_priority_message(type, payload, paused, pending_acks);
    }
}

void wait_for_sender_resume(int fd, bool& paused, std::deque<FileAckMessage>& pending_acks) {
    while (paused && pending_acks.empty()) {
        drain_sender_priority_events(fd, -1, paused, pending_acks);
    }
}

FileAckMessage wait_for_sender_ack(int fd, bool& paused, std::deque<FileAckMessage>& pending_acks) {
    if (!pending_acks.empty()) {
        const FileAckMessage ack = pending_acks.front();
        pending_acks.pop_front();
        return ack;
    }
    for (;;) {
        drain_sender_priority_events(fd, -1, paused, pending_acks);
        if (!pending_acks.empty()) {
            const FileAckMessage ack = pending_acks.front();
            pending_acks.pop_front();
            return ack;
        }
    }
}

std::size_t count_chunks_for_size(std::uint64_t total_bytes, std::size_t chunk_bytes) {
    if (chunk_bytes == 0U) {
        throw std::invalid_argument("chunk bytes must be positive");
    }
    if (total_bytes == 0U) {
        return 1U;
    }
    return static_cast<std::size_t>((total_bytes + static_cast<std::uint64_t>(chunk_bytes) - 1U) /
                                    static_cast<std::uint64_t>(chunk_bytes));
}

void populate_slot_for_chunk(DataSlotPool& pool,
                             const DataSlotHandle& handle,
                             const RecBuf& record,
                             std::uint64_t offset,
                             std::string_view bytes,
                             bool is_small,
                             bool is_last,
                             std::uint64_t data_hash) {
    DataBufTrailer& trailer = pool.trailer(handle);
    trailer.file_id = record.own_hash;
    trailer.folder_hash = record.folder_hash;
    trailer.data_offset = offset;
    trailer.data_len = bytes.size();
    trailer.file_size = record.size;
    trailer.data_hash = is_last ? data_hash : 0;
    trailer.mtime = record.mtime;
    trailer.mode = record.mode;
    trailer.uid = record.uid;
    trailer.gid = record.gid;
    trailer.chunk_hash = chunk_hash32(bytes);
    trailer.flags = is_small ? kFlagSmallFile : 0U;
    if (is_last) {
        trailer.flags |= kFlagLastChunk | kFlagHashValid;
    }
    trailer.rel_path = record.rel_path;
    trailer.slot_valid = kSlotValid;
}

struct SlotStreamingResult {
    std::uint64_t data_hash = 0;
    std::size_t chunk_count = 0;
};

template <typename Consumer>
SlotStreamingResult stream_content_slots(DataSlotPool& pool,
                                         const FileSpec& file,
                                         const EngineConfig& config,
                                         Consumer&& consume_slot) {
    const RecBuf record = make_recbuf(file);
    const bool is_small = record.size <= config.small_file_threshold;
    const std::size_t chunk_bytes = is_small ? config.small_file_threshold : config.large_chunk_bytes;
    Hash64State hasher;
    std::size_t chunk_count = 0;

    for (std::uint64_t offset = 0; offset < record.size || (record.size == 0 && offset == 0); offset += chunk_bytes) {
        const std::size_t len = record.size == 0
                                    ? 0
                                    : std::min<std::uint64_t>(static_cast<std::uint64_t>(chunk_bytes), record.size - offset);
        const std::string_view bytes(file.content.data() + static_cast<std::size_t>(offset), len);
        if (!bytes.empty()) {
            hasher.update(bytes);
        }

        DataSlotHandle handle = pool.acquire_or_throw(is_small ? DataSlotClass::small : DataSlotClass::large, len);
        if (len != 0U) {
            std::memcpy(pool.data(handle), bytes.data(), len);
        }
        const bool is_last = offset + len >= record.size;
        populate_slot_for_chunk(pool, handle, record, offset, bytes, is_small, is_last, hasher.value());
        consume_slot(handle);
        ++chunk_count;
    }

    return SlotStreamingResult{hasher.value(), chunk_count};
}

template <typename Consumer>
SlotStreamingResult stream_local_file_slots(DataSlotPool& pool,
                                            const std::filesystem::path& absolute_path,
                                            const FileSpec& file,
                                            const EngineConfig& config,
                                            Consumer&& consume_slot) {
    std::ifstream input(absolute_path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("failed to open source file: " + absolute_path.string());
    }

    const RecBuf record = make_recbuf(file);
    const bool is_small = record.size <= config.small_file_threshold;
    const std::size_t chunk_bytes = is_small ? config.small_file_threshold : config.large_chunk_bytes;
    Hash64State hasher;
    std::size_t chunk_count = 0;

    for (std::uint64_t offset = 0; offset < record.size || (record.size == 0 && offset == 0); offset += chunk_bytes) {
        const std::size_t len = record.size == 0
                                    ? 0
                                    : std::min<std::uint64_t>(static_cast<std::uint64_t>(chunk_bytes), record.size - offset);
        DataSlotHandle handle = pool.acquire_or_throw(is_small ? DataSlotClass::small : DataSlotClass::large, len);
        if (len != 0U) {
            input.read(pool.data(handle), static_cast<std::streamsize>(len));
            if (static_cast<std::size_t>(input.gcount()) != len) {
                pool.release(handle);
                throw std::runtime_error("failed to read source file chunk: " + absolute_path.string());
            }
            hasher.update(std::string_view(pool.data(handle), len));
        }
        const bool is_last = offset + len >= record.size;
        populate_slot_for_chunk(pool,
                                handle,
                                record,
                                offset,
                                std::string_view(pool.data(handle), len),
                                is_small,
                                is_last,
                                hasher.value());
        consume_slot(handle);
        ++chunk_count;
    }

    if (record.size != 0U && !input.eof()) {
        char extra = '\0';
        input.read(&extra, 1);
        if (input.gcount() != 0) {
            throw std::runtime_error("source file changed while streaming: " + absolute_path.string());
        }
    }

    return SlotStreamingResult{hasher.value(), chunk_count};
}

CachedFilePlan cache_file_for_transfer(DataCacher& cacher,
                                       DataSlotPool& slot_pool,
                                       const std::filesystem::path& absolute_path,
                                       const FileSpec& file,
                                       const EngineConfig& config) {
    const RecBuf record = make_recbuf(file);
    const SlotStreamingResult streamed =
        stream_local_file_slots(slot_pool, absolute_path, file, config, [&](const DataSlotHandle& handle) {
            cacher.cache_slot(slot_pool, handle);
            slot_pool.release(handle);
        });

    const std::uint64_t data_hash = streamed.data_hash;
    cacher.set_cached_file_hash(record.own_hash, data_hash);

    CachedFilePlan plan;
    plan.data_hash = data_hash;
    plan.chunk_count = streamed.chunk_count;
    plan.entry_ids = cacher.cached_entries_for(record.own_hash);
    return plan;
}

CachedFilePlan cache_file_for_transfer(DataCacher& cacher,
                                       DataSlotPool& slot_pool,
                                       const FileSpec& file,
                                       const EngineConfig& config) {
    const RecBuf record = make_recbuf(file);
    const SlotStreamingResult streamed = stream_content_slots(slot_pool, file, config, [&](const DataSlotHandle& handle) {
        cacher.cache_slot(slot_pool, handle);
        slot_pool.release(handle);
    });

    CachedFilePlan plan;
    plan.data_hash = streamed.data_hash;
    plan.chunk_count = streamed.chunk_count;
    plan.entry_ids = cacher.cached_entries_for(record.own_hash);
    return plan;
}

bool contains_path(const std::unordered_map<std::string, std::size_t>& map,
                   std::string_view path,
                   std::size_t attempt) {
    const auto it = map.find(normalize_path(path));
    if (it == map.end()) {
        return false;
    }
    return attempt <= it->second;
}

void send_content_file_slots(int data_fd,
                             DataSlotPool& slot_pool,
                             const FileSpec& file,
                             const EngineConfig& config,
                             int priority_fd,
                             bool& paused,
                             std::deque<FileAckMessage>& pending_acks,
                             TransferReport& report) {
    (void)stream_content_slots(slot_pool, file, config, [&](const DataSlotHandle& handle) {
        drain_sender_priority_events(priority_fd, 0, paused, pending_acks);
        wait_for_sender_resume(priority_fd, paused, pending_acks);
        send_data_slot(data_fd, slot_pool, handle);
        ++report.chunks_sent;
        slot_pool.release(handle);
    });
}

std::uint64_t send_local_file_slots(int data_fd,
                                    DataSlotPool& slot_pool,
                                    const std::filesystem::path& absolute_path,
                                    const FileSpec& file,
                                    const EngineConfig& config,
                                    int priority_fd,
                                    bool& paused,
                                    std::deque<FileAckMessage>& pending_acks,
                                    TransferReport& report) {
    return stream_local_file_slots(slot_pool, absolute_path, file, config, [&](const DataSlotHandle& handle) {
               drain_sender_priority_events(priority_fd, 0, paused, pending_acks);
               wait_for_sender_resume(priority_fd, paused, pending_acks);
               send_data_slot(data_fd, slot_pool, handle);
               ++report.chunks_sent;
               slot_pool.release(handle);
           })
        .data_hash;
}

std::uint64_t send_remote_nfs_file_slots(int data_fd,
                                         DataSlotPool& slot_pool,
                                         NfsDataReader& data_reader,
                                         const FileSpec& file,
                                         const EngineConfig& config,
                                         int priority_fd,
                                         bool& paused,
                                         std::deque<FileAckMessage>& pending_acks,
                                         TransferReport& report) {
    const RecBuf record = make_recbuf(file);
    const bool is_small = record.size <= config.small_file_threshold;
    Hash64State hasher;
    std::size_t chunk_count = 0;
    std::uint64_t logical_offset = 0;

    (void)data_reader.stream_file_pooled_chunks(file, slot_pool, [&](PooledFileChunk&& chunk) {
        DataSlotHandle handle = chunk.handle;
        try {
            DataBufTrailer& trailer = slot_pool.trailer(handle);
            const std::size_t len = trailer.data_len;
            if (chunk.offset != logical_offset || trailer.data_offset != logical_offset) {
                throw std::runtime_error("remote NFS source chunk offset mismatch for " + file.rel_path);
            }
            if (len != 0U) {
                hasher.update(std::string_view(slot_pool.data(handle), len));
            }
            logical_offset += len;
            const bool is_last = logical_offset >= record.size;
            populate_slot_for_chunk(slot_pool,
                                    handle,
                                    record,
                                    chunk.offset,
                                    std::string_view(slot_pool.data(handle), len),
                                    is_small,
                                    is_last,
                                    hasher.value());

            drain_sender_priority_events(priority_fd, 0, paused, pending_acks);
            wait_for_sender_resume(priority_fd, paused, pending_acks);
            send_data_slot(data_fd, slot_pool, handle);
            ++report.chunks_sent;
            ++chunk_count;
            slot_pool.release(handle);
        } catch (...) {
            slot_pool.release(handle);
            throw;
        }
    });

    if (record.size == 0 && chunk_count == 0) {
        DataSlotHandle handle = slot_pool.acquire_or_throw(DataSlotClass::small, 0);
        populate_slot_for_chunk(slot_pool,
                                handle,
                                record,
                                0,
                                std::string_view(slot_pool.data(handle), 0),
                                true,
                                true,
                                hasher.value());
        drain_sender_priority_events(priority_fd, 0, paused, pending_acks);
        wait_for_sender_resume(priority_fd, paused, pending_acks);
        send_data_slot(data_fd, slot_pool, handle);
        ++report.chunks_sent;
        slot_pool.release(handle);
        ++chunk_count;
    }

    if (logical_offset != record.size) {
        throw std::runtime_error("remote NFS source size mismatch for " + file.rel_path);
    }
    return hasher.value();
}

std::uint64_t write_packed_small_file_entry(char* payload,
                                            std::size_t& offset,
                                            const PreparedTransfer& prepared) {
    const std::uint64_t data_len = prepared.record.size;
    const std::uint32_t path_len = static_cast<std::uint32_t>(prepared.record.rel_path.size());
    const std::size_t entry_start = offset;
    const std::size_t data_start = entry_start + kPackedSmallFileEntryFixedBytes + path_len;
    char* const data_begin = payload + data_start;

    if (data_len != 0U) {
        if (prepared.content_loaded) {
            if (prepared.file.content.size() != data_len) {
                throw std::runtime_error("prepared small file content size mismatch");
            }
            std::memcpy(data_begin, prepared.file.content.data(), static_cast<std::size_t>(data_len));
        } else {
            std::ifstream input(prepared.source_path, std::ios::binary);
            if (!input) {
                throw std::runtime_error("failed to open source file: " + prepared.source_path.string());
            }
            input.read(data_begin, static_cast<std::streamsize>(data_len));
            if (input.gcount() != static_cast<std::streamsize>(data_len)) {
                throw std::runtime_error("failed to read complete source file: " + prepared.source_path.string());
            }
        }
    }

    const std::string_view data_view(data_begin, static_cast<std::size_t>(data_len));
    const std::uint64_t data_hash = hash64(data_view);

    std::size_t header = entry_start;
    write_u64_be(payload, header, prepared.record.file_id);
    write_u64_be(payload, header, data_len);
    write_u64_be(payload, header, prepared.record.size);
    write_u64_be(payload, header, data_hash);
    write_u64_be(payload, header, prepared.record.mtime);
    write_u32_be(payload, header, prepared.record.mode);
    write_u32_be(payload, header, prepared.record.uid);
    write_u32_be(payload, header, prepared.record.gid);
    write_u32_be(payload, header, path_len);
    if (path_len != 0U) {
        std::memcpy(payload + header, prepared.record.rel_path.data(), path_len);
        header += path_len;
    }
    if (header != data_start) {
        throw std::runtime_error("packed small file header size mismatch");
    }
    offset = data_start + static_cast<std::size_t>(data_len);
    return data_hash;
}

std::vector<std::uint64_t> send_packed_small_file_batch(int data_fd,
                                                       DataSlotPool& slot_pool,
                                                       std::vector<PreparedTransfer>& batch,
                                                       const EngineConfig& config,
                                                       int priority_fd,
                                                       bool& paused,
                                                       std::deque<FileAckMessage>& pending_acks,
                                                       TransferReport& report) {
    if (batch.empty()) {
        return {};
    }
    std::size_t payload_bytes = kPackedSmallFileCountBytes;
    for (const auto& prepared : batch) {
        payload_bytes += packed_small_file_entry_bytes(prepared);
    }
    if (payload_bytes > config.large_chunk_bytes) {
        throw std::runtime_error("packed small file batch exceeds large buffer capacity");
    }

    DataSlotHandle handle = slot_pool.acquire_or_throw(DataSlotClass::large, payload_bytes);
    std::vector<std::uint64_t> hashes;
    hashes.reserve(batch.size());
    try {
        char* payload = slot_pool.data(handle);
        std::size_t offset = kPackedSmallFileCountBytes;
        for (const auto& prepared : batch) {
            hashes.push_back(write_packed_small_file_entry(payload, offset, prepared));
        }
        if (offset != payload_bytes) {
            throw std::runtime_error("packed small file batch size mismatch");
        }
        write_u32_be_at(payload, 0, static_cast<std::uint32_t>(batch.size()));

        DataBufTrailer& trailer = slot_pool.trailer(handle);
        trailer.file_id = 0;
        trailer.folder_hash = 0;
        trailer.data_offset = 0;
        trailer.data_len = payload_bytes;
        trailer.file_size = payload_bytes;
        trailer.data_hash = 0;
        trailer.mtime = 0;
        trailer.mode = 0;
        trailer.uid = 0;
        trailer.gid = 0;
        trailer.chunk_hash = chunk_hash32(std::string_view(payload, payload_bytes));
        trailer.flags = kFlagPackedSmallFiles | kFlagLastChunk;
        trailer.rel_path.clear();
        trailer.slot_valid = kSlotValid;

        drain_sender_priority_events(priority_fd, 0, paused, pending_acks);
        wait_for_sender_resume(priority_fd, paused, pending_acks);
        send_data_slot(data_fd, slot_pool, handle);
        ++report.chunks_sent;
    } catch (...) {
        slot_pool.release(handle);
        throw;
    }
    slot_pool.release(handle);
    return hashes;
}

void record_completed_prepared_transfer(const PreparedTransfer& prepared,
                                        std::uint64_t data_hash,
                                        std::size_t chunk_count,
                                        const FileAckMessage& ack,
                                        TransferReport& report) {
    FileOutcome outcome;
    outcome.rel_path = prepared.file.rel_path;
    outcome.size = prepared.file.declared_size;
    outcome.sender_state = ack.hash_verified ? FileState::done : FileState::failed;
    outcome.receiver_state = ack.hash_verified ? FileState::done : FileState::failed;
    outcome.chunk_count = chunk_count;
    outcome.hash_verified = ack.hash_verified;
    outcome.data_hash = data_hash;

    FolderRecord& folder = report.folders[parent_path(prepared.file.rel_path)];
    if (!ack.hash_verified) {
        outcome.diff = DiffKind::failed;
        ++report.files_failed;
    } else {
        outcome.diff = DiffKind::new_file;
        ++report.files_transferred;
        report.bytes_transferred += ack.bytes_written;
        folder.bytes_transferred += ack.bytes_written;
        ++folder.files_completed;
        ++folder.files_received;
        ++folder.files_written;
        const auto source_snapshot = make_snapshot(prepared.file, outcome.data_hash, 'S');
        const auto target_snapshot = make_snapshot(prepared.file, outcome.data_hash, 'T');
        report.source_scan_rows.push_back(source_snapshot);
        report.target_scan_rows.push_back(target_snapshot);
    }
    report.files[prepared.file.rel_path] = outcome;
}

void record_sender_ack(const FileAckMessage& ack,
                       std::unordered_map<std::uint64_t, AwaitingAckTransfer>& awaiting_acks,
                       TransferReport& report) {
    const auto it = awaiting_acks.find(ack.file_id);
    if (it == awaiting_acks.end()) {
        throw std::runtime_error("received ack for unknown file_id: " + std::to_string(ack.file_id));
    }
    record_completed_prepared_transfer(it->second.prepared,
                                       it->second.data_hash,
                                       it->second.chunk_count,
                                       ack,
                                       report);
    awaiting_acks.erase(it);
}

void consume_sender_acks(std::deque<FileAckMessage>& pending_acks,
                         std::unordered_map<std::uint64_t, AwaitingAckTransfer>& awaiting_acks,
                         TransferReport& report) {
    while (!pending_acks.empty()) {
        const FileAckMessage ack = pending_acks.front();
        pending_acks.pop_front();
        record_sender_ack(ack, awaiting_acks, report);
    }
}

PreparedTransfer prepare_transfer_file(const PendingTransferFile& pending,
                                       bool remote_source,
                                       const std::filesystem::path& source_root,
                                       NfsDataReader& data_reader,
                                       DataCacher& cacher,
                                       DataSlotPool& cache_slots,
                                       const EngineConfig& config,
                                       std::uint64_t cache_file_threshold_bytes,
                                       bool enable_cache) {
    PreparedTransfer prepared;
    prepared.file = pending.file;
    prepared.record = pending.record;

    if (enable_cache &&
        (cache_file_threshold_bytes == 0 || prepared.file.declared_size >= cache_file_threshold_bytes)) {
        if (remote_source) {
            FileSpec loaded = data_reader.load_file(prepared.file.rel_path);
            prepared.file.content = std::move(loaded.content);
            prepared.content_loaded = true;
            const CachedFilePlan plan = cache_file_for_transfer(cacher, cache_slots, prepared.file, config);
            prepared.cached = true;
            prepared.data_hash = plan.data_hash;
            prepared.chunk_count = plan.chunk_count;
            prepared.cached_entry_ids = plan.entry_ids;
            return prepared;
        }

        prepared.source_path = source_root / prepared.file.rel_path;
        const CachedFilePlan plan = cache_file_for_transfer(cacher, cache_slots, prepared.source_path, prepared.file, config);
        prepared.cached = true;
        prepared.data_hash = plan.data_hash;
        prepared.chunk_count = plan.chunk_count;
        prepared.cached_entry_ids = plan.entry_ids;
        return prepared;
    }

    if (remote_source && prepared.record.size <= config.small_file_threshold) {
        prepared.file = data_reader.load_file(prepared.file.rel_path);
        prepared.content_loaded = true;
    } else if (!remote_source) {
        prepared.source_path = source_root / prepared.file.rel_path;
    }

    const bool is_small = prepared.record.size <= config.small_file_threshold;
    const std::size_t chunk_bytes = is_small ? config.small_file_threshold : config.large_chunk_bytes;
    prepared.chunk_count = count_chunks_for_size(prepared.record.size, chunk_bytes);
    if (prepared.content_loaded) {
        prepared.data_hash = hash64(prepared.file.content);
    }
    return prepared;
}

void produce_prepared_transfers(PreparedTransferQueue& queue,
                                const std::vector<PendingTransferFile>& pending_files,
                                bool remote_source,
                                const std::filesystem::path& source_root,
                                NfsDataReader& data_reader,
                                DataCacher& cacher,
                                DataSlotPool& cache_slots,
                                const EngineConfig& config,
                                std::uint64_t cache_file_threshold_bytes,
                                bool enable_cache) {
    try {
        for (const auto& pending : pending_files) {
            PreparedTransfer prepared = prepare_transfer_file(pending,
                                                             remote_source,
                                                             source_root,
                                                             data_reader,
                                                             cacher,
                                                             cache_slots,
                                                             config,
                                                             cache_file_threshold_bytes,
                                                             enable_cache);

            std::unique_lock<std::mutex> lock(queue.mutex);
            queue.cv_not_full.wait(lock, [&queue] {
                return queue.stop || !queue.queue.full();
            });
            if (queue.stop) {
                break;
            }
            queue.queue.push(std::move(prepared));
            queue.cv_not_empty.notify_one();
        }
    } catch (...) {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.producer_error = std::current_exception();
    }

    std::lock_guard<std::mutex> lock(queue.mutex);
    queue.input_done = true;
    queue.cv_not_empty.notify_all();
    queue.cv_not_full.notify_all();
}

bool pop_prepared_transfer(PreparedTransferQueue& queue, PreparedTransfer& prepared) {
    std::unique_lock<std::mutex> lock(queue.mutex);
    queue.cv_not_empty.wait(lock, [&queue] {
        return queue.producer_error != nullptr || queue.input_done || !queue.queue.empty();
    });
    if (queue.producer_error != nullptr) {
        std::rethrow_exception(queue.producer_error);
    }
    if (queue.queue.empty()) {
        return false;
    }

    prepared = queue.queue.pop();
    queue.cv_not_full.notify_one();
    return true;
}

bool try_pop_prepared_transfer(PreparedTransferQueue& queue, PreparedTransfer& prepared) {
    std::unique_lock<std::mutex> lock(queue.mutex);
    if (queue.producer_error != nullptr) {
        std::rethrow_exception(queue.producer_error);
    }
    if (queue.queue.empty()) {
        return false;
    }
    prepared = queue.queue.pop();
    lock.unlock();
    queue.cv_not_full.notify_one();
    return true;
}

void write_text_file(const std::filesystem::path& path, const std::string& contents) {
    ensure_parent_directories(path);
    std::ofstream output(path, std::ios::binary);
    if (!output) {
        throw std::runtime_error("failed to open output file: " + path.string());
    }
    output.write(contents.data(), static_cast<std::streamsize>(contents.size()));
    if (!output) {
        throw std::runtime_error("failed to write output file: " + path.string());
    }
}

std::string read_text_file(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("failed to open input file: " + path.string());
    }
    return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
}

}  // namespace

SplitBucketPriorityDecision choose_split_bucket_priority_workers(
    const SplitBucketPriorityInput& input) noexcept {
    return choose_split_bucket_priority_workers_impl(input);
}

SplitScannerCapacityDecision choose_split_scanner_capacity(
    std::size_t base_small_scanners,
    std::size_t base_large_scanners,
    std::size_t large_scanner_floor,
    std::uint32_t large_reader_small_priority_percent) noexcept {
    return choose_split_scanner_capacity_impl(base_small_scanners,
                                              base_large_scanners,
                                              large_scanner_floor,
                                              large_reader_small_priority_percent);
}

BucketPathOverloadScores evaluate_bucket_path_overload(
    const BucketPathOverloadInput& input) noexcept {
    return evaluate_bucket_path_overload_impl(input);
}

SenderRuntimeConfig::SenderRuntimeConfig()
    : SenderRuntimeConfig(load_sender_runtime_config(ConfigStore{})) {}

SenderRuntimeConfig::SenderRuntimeConfig(std::filesystem::path source_root,
                                         std::string remote_host,
                                         std::uint16_t priority_port,
                                         std::uint16_t data_port,
                                         bool recursive,
                                         std::filesystem::path cache_root,
                                         std::uint64_t cache_file_threshold_bytes)
    : source_root(std::move(source_root)),
      remote_host(std::move(remote_host)),
      priority_port(priority_port),
      data_port(data_port),
      recursive(recursive),
      cache_root(std::move(cache_root)),
      cache_file_threshold_bytes(cache_file_threshold_bytes) {}

ReceiverRuntimeConfig::ReceiverRuntimeConfig()
    : ReceiverRuntimeConfig(load_receiver_runtime_config(ConfigStore{})) {}

ReceiverRuntimeConfig::ReceiverRuntimeConfig(std::filesystem::path target_root,
                                             std::string bind_host,
                                             std::uint16_t priority_port,
                                             std::uint16_t data_port,
                                             std::uint64_t backpressure_window_bytes,
                                             std::uint32_t backpressure_pause_ms)
    : target_root(std::move(target_root)),
      bind_host(std::move(bind_host)),
      priority_port(priority_port),
      data_port(data_port),
      backpressure_window_bytes(backpressure_window_bytes),
      backpressure_pause_ms(backpressure_pause_ms) {}

SenderRuntimeConfig load_sender_runtime_config(const ConfigStore& config) {
    const ConfigSection values = config.section("runtime.sender");
    return SenderRuntimeConfig({},
                               config_string(values, "remote_host"),
                               config_u16(values, "priority_port"),
                               config_u16(values, "data_port"),
                               config_bool(values, "recursive"),
                               {},
                               config_u64(values, "cache_file_threshold_bytes"));
}

ReceiverRuntimeConfig load_receiver_runtime_config(const ConfigStore& config) {
    const ConfigSection values = config.section("runtime.receiver");
    return ReceiverRuntimeConfig({},
                                 config_string(values, "bind_host"),
                                 config_u16(values, "priority_port"),
                                 config_u16(values, "data_port"),
                                 config_u64(values, "backpressure_window_bytes"),
                                 config_u32(values, "backpressure_pause_ms"));
}

TransferEngine::TransferEngine(EngineConfig config, ConfigStore config_store)
    : config_(std::move(config)),
      config_store_(std::move(config_store)) {
    if (config_.small_file_threshold == 0 || config_.large_chunk_bytes == 0) {
        throw std::invalid_argument("chunk sizes must be positive");
    }
    if (config_.small_file_threshold > kSmallFileThreshold) {
        throw std::invalid_argument("small file threshold exceeds fixed slot capacity");
    }
    if (config_.large_chunk_bytes > kLargeChunkBytes) {
        throw std::invalid_argument("large chunk bytes exceed fixed slot capacity");
    }
}

TransferReport TransferEngine::run(const std::vector<FileSpec>& source_files,
                                   const ScanIndex* source_scan,
                                   const ScanIndex* target_scan) const {
    DataSlotPool slot_pool(config_.small_pool_slots, config_.large_pool_slots);

    std::vector<FileSpec> normalized_files = source_files;
    for (auto& file : normalized_files) {
        file.rel_path = normalize_path(file.rel_path);
    }
    std::sort(normalized_files.begin(), normalized_files.end(), [](const FileSpec& lhs, const FileSpec& rhs) {
        return lhs.rel_path < rhs.rel_path;
    });

    std::map<std::string, std::vector<FileSpec>> files_by_folder;
    for (const auto& file : normalized_files) {
        files_by_folder[parent_path(file.rel_path)].push_back(file);
    }

    TransferReport report;
    report.mode = config_.mode;
    report.files_total = normalized_files.size();
    report.folders_total = files_by_folder.size();
    if (config_.mode == Mode::dry_run) {
        report.diff_csv = "rel_path,decision,size,mtime\n";
    }

    for (const auto& [folder_path, files] : files_by_folder) {
        FolderRecord folder;
        folder.rel_path = folder_path;
        folder.parent_hash = folder_hash_for_path(parent_path(folder_path));
        folder.md_hash = path_hash(base_name(folder_path), folder.parent_hash);
        folder.files_discovered = files.size();
        folder.files_total = files.size();
        folder.started_ts = files.empty() ? 0 : files.front().mtime;

        transition_folder(folder, EndpointRole::sender, FolderState::reading);
        if (config_.mode == Mode::transfer) {
            transition_folder(folder, EndpointRole::receiver, FolderState::receiving);
        }

        const std::uint64_t source_folder_hash = source_scan ? source_scan->folder_data_hash(folder_path) : 0;
        const std::uint64_t target_folder_hash = target_scan ? target_scan->folder_data_hash(folder_path) : 0;
        const bool folder_fast_skip =
            source_scan != nullptr && target_scan != nullptr && source_folder_hash != 0 && source_folder_hash == target_folder_hash;

        std::vector<FileSnapshot> folder_snapshots;
        bool any_transfer = false;
        bool any_receiver_write = false;

        for (const auto& file : files) {
            RecBuf record = make_recbuf(file);
            FileOutcome outcome;
            outcome.rel_path = file.rel_path;
            outcome.size = record.size;

            const auto known_source = source_scan ? source_scan->find(file.rel_path) : std::nullopt;
            const auto known_target = target_scan ? target_scan->find(file.rel_path) : std::nullopt;

            bool skip = false;
            if (config_.mode != Mode::scan) {
                if (folder_fast_skip) {
                    skip = true;
                } else if (known_source.has_value() && target_scan != nullptr && target_scan->file_matches(*known_source)) {
                    skip = true;
                } else if (!known_source.has_value() && target_scan != nullptr &&
                           target_scan->metadata_matches(file.rel_path, record.size, record.mtime)) {
                    skip = true;
                }
            }

            if (config_.mode == Mode::scan) {
                transition_file(record, EndpointRole::sender, FileState::reading);
                const std::uint64_t data_hash = hash64(file.content);
                record.data_hash = data_hash;
                outcome.data_hash = data_hash;
                outcome.hash_verified = true;
                transition_file(record, EndpointRole::sender, FileState::done);
                outcome.sender_state = record.state;
                outcome.receiver_state = FileState::pending;
                report.files[file.rel_path] = outcome;
                const auto snapshot = make_snapshot(file, data_hash, 'S');
                report.source_scan_rows.push_back(snapshot);
                folder_snapshots.push_back(snapshot);
                folder.files_completed++;
                folder.flat_size_bytes += record.size;
                continue;
            }

            transition_file(record, EndpointRole::sender, FileState::checking);
            if (skip) {
                transition_file(record, EndpointRole::sender, FileState::skipped);
                transition_file(record, EndpointRole::sender, FileState::done);
                outcome.diff = DiffKind::skip;
                outcome.sender_state = record.state;
                outcome.receiver_state = FileState::done;
                outcome.hash_verified = true;
                outcome.data_hash = known_source ? known_source->data_hash : (known_target ? known_target->data_hash : 0);
                report.files_skipped++;
                folder.files_skipped++;
                folder.files_completed++;
                folder.files_received++;
                if (known_source.has_value()) {
                    report.source_scan_rows.push_back(*known_source);
                    folder_snapshots.push_back(*known_source);
                }
                if (known_target.has_value()) {
                    report.target_scan_rows.push_back(*known_target);
                }
                if (config_.mode == Mode::dry_run) {
                    report.diff_csv += file.rel_path + "," + to_string(DiffKind::skip) + "," +
                                       std::to_string(record.size) + "," + std::to_string(record.mtime) + "\n";
                }
                report.files[file.rel_path] = outcome;
                folder.flat_size_bytes += record.size;
                continue;
            }

            if (config_.mode == Mode::dry_run) {
                outcome.diff = known_target.has_value() ? DiffKind::changed : DiffKind::new_file;
                outcome.sender_state = record.state;
                outcome.receiver_state = FileState::pending;
                report.bytes_planned += record.size;
                report.diff_csv += file.rel_path + "," + to_string(outcome.diff) + "," +
                                   std::to_string(record.size) + "," + std::to_string(record.mtime) + "\n";
                report.files[file.rel_path] = outcome;
                folder.flat_size_bytes += record.size;
                continue;
            }

            const bool is_small = record.size <= config_.small_file_threshold;
            const std::size_t chunk_size = is_small ? config_.small_file_threshold : config_.large_chunk_bytes;
            const std::uint64_t data_hash = known_source ? known_source->data_hash : hash64(file.content);
            const std::size_t total_attempts = config_.max_retries + 1;
            std::size_t chunks_for_success = 0;
            bool transferred = false;
            FileState receiver_state = FileState::pending;

            report.bytes_planned += record.size;
            any_transfer = true;

            for (std::size_t attempt = 1; attempt <= total_attempts; ++attempt) {
                if (record.state == FileState::checking || record.state == FileState::failed) {
                    transition_file(record, EndpointRole::sender, FileState::reading);
                }
                if (receiver_state == FileState::pending) {
                    if (can_transition_file(EndpointRole::receiver, receiver_state, FileState::receiving)) {
                        receiver_state = FileState::receiving;
                    }
                } else if (receiver_state == FileState::failed) {
                    receiver_state = FileState::receiving;
                }

                outcome.attempts = attempt;
                std::size_t chunk_count = 0;
                for (std::size_t offset = 0; offset < file.content.size() || (file.content.empty() && offset == 0); offset += chunk_size) {
                    const std::size_t remaining = file.content.size() > offset ? file.content.size() - offset : 0;
                    const std::size_t len = file.content.empty() ? 0 : std::min<std::size_t>(chunk_size, remaining);
                    const std::string_view bytes(file.content.data() + offset, len);
                    const DataSlotHandle handle =
                        slot_pool.acquire_or_throw(is_small ? DataSlotClass::small : DataSlotClass::large, len);
                    if (len != 0U) {
                        std::memcpy(slot_pool.data(handle), bytes.data(), len);
                    }
                    populate_slot_for_chunk(slot_pool,
                                            handle,
                                            record,
                                            offset,
                                            bytes,
                                            is_small,
                                            offset + len >= file.content.size(),
                                            data_hash);

                    record.bytes_sent += len;
                    ++chunk_count;
                    ++report.chunks_sent;
                    slot_pool.release(handle);
                }

                if (record.state == FileState::checking || record.state == FileState::failed) {
                    transition_file(record, EndpointRole::sender, FileState::reading);
                }
                transition_file(record, EndpointRole::sender, FileState::transferring);
                receiver_state = FileState::writing;
                const bool forced_failure = !config_.skip_verify &&
                                            contains_path(config_.forced_failures, file.rel_path, attempt);
                if (forced_failure) {
                    transition_file(record, EndpointRole::sender, FileState::failed);
                    receiver_state = FileState::failed;
                    if (attempt < total_attempts) {
                        ++report.retries;
                    }
                    continue;
                }

                transition_file(record, EndpointRole::sender, FileState::done);
                receiver_state = FileState::done;
                transferred = true;
                chunks_for_success = chunk_count;
                break;
            }

            if (!transferred) {
                outcome.diff = DiffKind::failed;
                outcome.sender_state = record.state;
                outcome.receiver_state = receiver_state;
                outcome.data_hash = data_hash;
                outcome.hash_verified = false;
                report.files_failed++;
                report.files[file.rel_path] = outcome;
                folder.flat_size_bytes += record.size;
                continue;
            }

            outcome.diff = known_target.has_value() ? DiffKind::changed : DiffKind::new_file;
            outcome.sender_state = record.state;
            outcome.receiver_state = receiver_state;
            outcome.data_hash = data_hash;
            outcome.hash_verified = true;
            outcome.chunk_count = chunks_for_success;
            report.files_transferred++;
            report.bytes_transferred += record.size;
            folder.bytes_transferred += record.size;
            folder.files_completed++;
            folder.files_received++;
            folder.files_written++;
            any_receiver_write = true;

            const auto source_snapshot = known_source ? *known_source : make_snapshot(file, data_hash, 'S');
            const auto target_snapshot = make_snapshot(file, data_hash, 'T');
            report.source_scan_rows.push_back(source_snapshot);
            report.target_scan_rows.push_back(target_snapshot);
            folder_snapshots.push_back(source_snapshot);
            report.files[file.rel_path] = outcome;
            folder.flat_size_bytes += record.size;
        }

        folder.folder_data_hash = compute_folder_data_hash(folder_snapshots);
        folder.last_scan_ts = files.empty() ? 0 : files.back().mtime;
        folder.completed_ts = folder.last_scan_ts;

        if (config_.mode == Mode::scan) {
            transition_folder(folder, EndpointRole::sender, FolderState::done);
        } else {
            if (any_transfer) {
                transition_folder(folder, EndpointRole::sender, FolderState::transferring);
                transition_folder(folder, EndpointRole::sender, FolderState::awaiting_ack);
            } else {
                transition_folder(folder, EndpointRole::sender, FolderState::awaiting_ack);
            }
            transition_folder(folder, EndpointRole::sender, FolderState::done);
        }

        if (config_.mode == Mode::transfer) {
            if (any_receiver_write) {
                transition_folder(folder, EndpointRole::receiver, FolderState::writing);
            }
            transition_folder(folder, EndpointRole::receiver, FolderState::done);
        }

        report.folders[folder_path] = folder;
    }

    std::sort(report.source_scan_rows.begin(), report.source_scan_rows.end(), [](const FileSnapshot& lhs, const FileSnapshot& rhs) {
        return lhs.rel_path < rhs.rel_path;
    });
    std::sort(report.target_scan_rows.begin(), report.target_scan_rows.end(), [](const FileSnapshot& lhs, const FileSnapshot& rhs) {
        return lhs.rel_path < rhs.rel_path;
    });

    return report;
}

std::vector<FileSpec> TransferEngine::scan_directory(const std::filesystem::path& source_root, bool recursive) {
    if (is_nfs_url(source_root.string())) {
        NfsMetaReaderConfig reader_config = load_nfs_meta_reader_config(ConfigStore{});
        reader_config.source_root = source_root.string();
        reader_config.recursive = recursive;
        NfsMetaReader reader(reader_config);
        return reader.scan_tree();
    }
    return collect_file_specs(source_root, recursive);
}

std::vector<FileSpec> TransferEngine::scan_directory_with_config(const std::filesystem::path& source_root, bool recursive) const {
    if (is_nfs_url(source_root.string())) {
        NfsMetaReaderConfig reader_config = load_nfs_meta_reader_config(config_store_);
        reader_config.source_root = source_root.string();
        reader_config.recursive = recursive;
        NfsMetaReader reader(reader_config);
        return reader.scan_tree();
    }
    return collect_file_specs(source_root, recursive);
}

TransferReport TransferEngine::scan_directory_report(const std::filesystem::path& source_root,
                                                     char scan_side,
                                                     bool recursive) const {
    auto files = scan_directory_with_config(source_root, recursive);
    if (is_nfs_url(source_root.string())) {
        NfsDataReaderConfig reader_config = load_nfs_data_reader_config(config_store_);
        reader_config.small_file_threshold = config_.small_file_threshold;
        reader_config.large_chunk_bytes = config_.large_chunk_bytes;
        reader_config.source_root = source_root.string();
        NfsDataReader reader(reader_config);
        for (auto& file : files) {
            file.content = reader.load_file(file.rel_path).content;
            if (file.declared_size == 0) {
                file.declared_size = file.content.size();
            }
        }
    } else {
        for (auto& file : files) {
            file.content = read_file_contents(source_root / file.rel_path);
            if (file.declared_size == 0) {
                file.declared_size = file.content.size();
            }
        }
    }

    EngineConfig scan_config = config_;
    scan_config.mode = Mode::scan;
    TransferEngine scan_engine(scan_config);
    TransferReport report = scan_engine.run(files);

    for (auto& snapshot : report.source_scan_rows) {
        snapshot.scan_side = scan_side;
    }
    if (scan_side == 'T') {
        report.target_scan_rows = report.source_scan_rows;
        report.source_scan_rows.clear();
    }
    return report;
}

TransferReport TransferEngine::dry_run_directory(const std::filesystem::path& source_root,
                                                 const ScanIndex* source_scan,
                                                 const ScanIndex* target_scan,
                                                 bool recursive) const {
    auto files = scan_directory_with_config(source_root, recursive);

    std::optional<ScanIndex> computed_source_scan;
    const ScanIndex* effective_source_scan = source_scan;
    if (effective_source_scan == nullptr && target_scan != nullptr) {
        computed_source_scan.emplace(build_scan_index(source_root, 'S', recursive));
        effective_source_scan = &*computed_source_scan;
    }

    EngineConfig dry_run_config = config_;
    dry_run_config.mode = Mode::dry_run;
    TransferEngine dry_run_engine(dry_run_config);
    return dry_run_engine.run(files, effective_source_scan, target_scan);
}

TransferReport TransferEngine::diff_scan_indexes(const ScanIndex& source_scan,
                                                 const ScanIndex& target_scan,
                                                 const std::string& compare_mode) const {
    if (compare_mode != "size" && compare_mode != "time" && compare_mode != "content") {
        throw std::invalid_argument("diff compare mode must be size, time, or content");
    }

    const auto source_rows = source_scan.rows();
    const auto target_rows = target_scan.rows();
    std::map<std::string, FileSnapshot> target_by_path;
    for (const auto& row : target_rows) {
        target_by_path.emplace(row.rel_path, row);
    }

    const auto rows_match = [&](const FileSnapshot& source, const FileSnapshot& target) {
        if (compare_mode == "size") {
            return source.size == target.size;
        }
        if (compare_mode == "time") {
            return source.size == target.size && source.mtime == target.mtime;
        }
        if (source.data_hash != 0U && target.data_hash != 0U) {
            return source.size == target.size && source.data_hash == target.data_hash;
        }
        return source.size == target.size && source.mtime == target.mtime;
    };

    TransferReport report;
    report.mode = Mode::dry_run;
    report.source_scan_rows = source_rows;
    report.target_scan_rows = target_rows;
    report.diff_csv = "rel_path,decision,source_size,source_mtime,target_size,target_mtime\n";

    for (const auto& source : source_rows) {
        FileOutcome outcome;
        outcome.rel_path = source.rel_path;
        outcome.size = source.size;
        outcome.data_hash = source.data_hash;
        const auto target_it = target_by_path.find(source.rel_path);
        if (target_it == target_by_path.end()) {
            outcome.diff = DiffKind::new_file;
            ++report.files_new;
            report.bytes_planned += source.size;
        } else if (rows_match(source, target_it->second)) {
            outcome.diff = DiffKind::skip;
            ++report.files_skipped;
        } else {
            outcome.diff = DiffKind::changed;
            ++report.files_changed;
            report.bytes_planned += source.size;
        }
        const std::uint64_t target_size = target_it == target_by_path.end() ? 0U : target_it->second.size;
        const std::uint64_t target_mtime = target_it == target_by_path.end() ? 0U : target_it->second.mtime;
        report.diff_csv += source.rel_path + "," + to_string(outcome.diff) + "," +
                           std::to_string(source.size) + "," + std::to_string(source.mtime) + "," +
                           std::to_string(target_size) + "," + std::to_string(target_mtime) + "\n";
        report.files[source.rel_path] = outcome;
        if (target_it != target_by_path.end()) {
            target_by_path.erase(target_it);
        }
    }

    for (const auto& [path, target] : target_by_path) {
        FileOutcome outcome;
        outcome.rel_path = path;
        outcome.diff = DiffKind::target_only;
        outcome.size = target.size;
        outcome.data_hash = target.data_hash;
        ++report.files_target_only;
        report.diff_csv += path + "," + to_string(DiffKind::target_only) + ",0,0," +
                           std::to_string(target.size) + "," + std::to_string(target.mtime) + "\n";
        report.files[path] = outcome;
    }

    report.files_total = report.files.size();
    return report;
}

TransferReport TransferEngine::diff_metadata_trees(const std::filesystem::path& source_root,
                                                   const std::filesystem::path& target_root,
                                                   const std::string& compare_mode,
                                                   bool recursive,
                                                   std::size_t meta_reader_threads,
                                                   std::size_t metadata_async_depth,
                                                   double max_duration_seconds,
                                                   bool collect_detailed_records,
                                                   std::uint32_t stats_interval_seconds,
                                                   std::size_t checker_threads,
                                                   std::size_t checker_request_queue_depth,
                                                   std::size_t checker_batch_queue_depth) const {
    if (compare_mode != "size" && compare_mode != "time" && compare_mode != "content") {
        throw std::invalid_argument("diff compare mode must be size, time, or content");
    }
    if (source_root.empty()) {
        throw std::invalid_argument("source root is required");
    }
    if (target_root.empty()) {
        throw std::invalid_argument("target root is required");
    }

    NfsMetaReaderConfig reader_config = load_nfs_meta_reader_config(config_store_);
    reader_config.source_root = source_root.string();
    reader_config.recursive = recursive;
    if (meta_reader_threads != 0) {
        reader_config.worker_count = meta_reader_threads;
        reader_config.thread_count = meta_reader_threads;
    }
    if (metadata_async_depth != 0) {
        reader_config.async_directory_depth = metadata_async_depth;
    }
    const CheckerConfig checker_config = load_checker_config(config_store_);
    const std::size_t effective_checker_threads =
        checker_threads != 0U ? checker_threads : checker_config.worker_count;
    const std::size_t effective_checker_request_queue_depth =
        checker_request_queue_depth != 0U ? checker_request_queue_depth : checker_config.target_request_queue_depth;
    const std::size_t effective_checker_batch_queue_depth =
        checker_batch_queue_depth != 0U ? checker_batch_queue_depth : checker_config.batch_queue_depth;

    if (!collect_detailed_records) {
        return run_summary_live_diff(reader_config.source_root,
                                     target_root.string(),
                                     compare_mode,
                                     recursive,
                                     reader_config,
                                     max_duration_seconds,
                                     stats_interval_seconds,
                                     effective_checker_threads,
                                     effective_checker_request_queue_depth,
                                     effective_checker_batch_queue_depth);
    }

    TransferReport report;
    report.mode = Mode::dry_run;
    if (collect_detailed_records) {
        report.diff_csv = "rel_path,decision,source_size,source_mtime,target_size,target_mtime\n";
    }

    FlatMetadataWorkQueue queue;
    queue.folders.push_back(FileSpec{});
    if (max_duration_seconds > 0.0) {
        queue.stop_at = std::chrono::steady_clock::now() +
                        std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                            std::chrono::duration<double>(max_duration_seconds));
    }

    std::mutex report_mutex;
    const std::size_t thread_count = std::max<std::size_t>(1, reader_config.worker_count);
    std::vector<std::thread> workers;
    workers.reserve(thread_count);
    for (std::size_t index = 0; index < thread_count; ++index) {
        workers.emplace_back(live_diff_metadata_worker,
                             reader_config.source_root,
                             target_root.string(),
                             recursive,
                             compare_mode,
                             std::max<std::size_t>(1, reader_config.async_directory_depth),
                             std::ref(queue),
                             std::ref(report),
                             std::ref(report_mutex),
                             collect_detailed_records);
    }
    for (auto& worker : workers) {
        worker.join();
    }
    if (queue.error) {
        std::rethrow_exception(queue.error);
    }

    if (collect_detailed_records) {
        report.files_total = report.files.size();
        report.folders_total = report.folders.size();
    }
    return report;
}

ScanIndex TransferEngine::build_scan_index(const std::filesystem::path& source_root,
                                           char scan_side,
                                           bool recursive) const {
    const TransferReport report = scan_directory_report(source_root, scan_side, recursive);
    ScanIndex index;
    const auto& rows = scan_side == 'T' ? report.target_scan_rows : report.source_scan_rows;
    for (const auto& row : rows) {
        index.add(row);
    }
    return index;
}

MetadataBenchmarkReport TransferEngine::benchmark_metadata_pipeline(const std::filesystem::path& source_root,
                                                                    bool recursive,
                                                                    bool discard_after_checker,
                                                                    bool use_stats_discarder,
                                                                    std::size_t meta_reader_threads,
                                                                    std::size_t metadata_async_depth,
                                                                    const std::filesystem::path& metadata_output_path,
                                                                    const std::string& metadata_output_format,
                                                                    const std::string& metadata_records,
                                                                    double max_duration_seconds,
                                                                    std::uint32_t stats_interval_seconds,
                                                                    std::size_t record_buffer_slots,
                                                                    std::size_t metadata_output_partitions,
                                                                    const std::string& metadata_output_partition_mode,
                                                                    const std::filesystem::path& status_socket_path,
                                                                    bool pipeline_autoscale,
                                                                    std::string autoscale_profile,
                                                                    std::filesystem::path autoscale_settings_path,
                                                                    std::uint64_t autoscale_interval_ms) const {
    (void)discard_after_checker;
    NfsMetaReaderConfig reader_config = load_nfs_meta_reader_config(config_store_);
    reader_config.source_root = source_root.string();
    reader_config.recursive = recursive;
    if (meta_reader_threads != 0) {
        reader_config.worker_count = meta_reader_threads;
        reader_config.thread_count = meta_reader_threads;
    }
    if (metadata_async_depth != 0) {
        reader_config.async_directory_depth = metadata_async_depth;
    }
    if (record_buffer_slots != 0) {
        reader_config.recbuf_window = record_buffer_slots;
    }
    if (pipeline_autoscale) {
        if (meta_reader_threads == 0U) {
            reader_config.worker_count = default_autoscale_max_workers();
            reader_config.thread_count = reader_config.worker_count;
        }
        if (autoscale_profile.empty()) {
            autoscale_profile = "scan_metadata";
        }
        if (autoscale_settings_path.empty()) {
            autoscale_settings_path = default_autoscale_settings_path();
        }
        autoscale_interval_ms = std::max<std::uint64_t>(100U, autoscale_interval_ms);
    }
    NfsMetaReader reader(reader_config);

    MetadataBenchmarkReport report;
    report.meta_reader_async = reader.using_async_backend();
    report.meta_reader_threads = std::max<std::size_t>(1, reader_config.worker_count);
    report.metadata_async_depth = std::max<std::size_t>(1, reader_config.async_directory_depth);
    report.record_buffer_slots = reader_config.recbuf_window;
    report.stats_interval_seconds = std::max<std::uint32_t>(1, stats_interval_seconds);
    report.metadata_output_partitions = std::max<std::size_t>(1U, metadata_output_partitions);
    report.pipeline_autoscale = pipeline_autoscale;
    report.autoscale_interval_ms = pipeline_autoscale ? autoscale_interval_ms : 0U;
    report.autoscale_profile = pipeline_autoscale ? autoscale_profile : std::string {};
    report.autoscale_settings_path = pipeline_autoscale ? autoscale_settings_path : std::filesystem::path {};
    if (metadata_output_partition_mode != "single" &&
        metadata_output_partition_mode != "processes" &&
        metadata_output_partition_mode != "transport-discard" &&
        metadata_output_partition_mode != "route-discard" &&
        metadata_output_partition_mode != "sharded-discard") {
        throw std::invalid_argument("metadata output partition mode must be single, processes, transport-discard, route-discard, or sharded-discard");
    }
    const bool sharded_discard = metadata_output_partition_mode == "sharded-discard";

    const auto started = std::chrono::steady_clock::now();

    MetadataStatsDiscarderConfig stats_config = load_metadata_stats_discarder_config(config_store_);
    stats_config.enabled = stats_config.enabled || use_stats_discarder || !status_socket_path.empty();
    stats_config.print_interval_seconds = report.stats_interval_seconds;
    MetadataStatsDiscarder stats_discarder(stats_config);
    if (stats_config.enabled) {
        MetadataRecordWriterConfig writer_config = load_metadata_record_writer_config(config_store_);
        if (!metadata_output_path.empty()) {
            writer_config.enabled = true;
            writer_config.output_path = metadata_output_path;
            if (metadata_output_format.empty()) {
                writer_config.format = infer_metadata_record_format(metadata_output_path);
            }
        }
        if (!metadata_output_format.empty()) {
            writer_config.format = parse_metadata_record_format(metadata_output_format);
        }
        if (!metadata_records.empty()) {
            if (metadata_records == "all") {
                writer_config.write_files = true;
                writer_config.write_folders = true;
            } else if (metadata_records == "files") {
                writer_config.write_files = true;
                writer_config.write_folders = false;
            } else if (metadata_records == "folders") {
                writer_config.write_files = false;
                writer_config.write_folders = true;
            } else {
                throw std::invalid_argument("metadata records must be all, files, or folders");
            }
        }
        const std::string effective_records = writer_config.write_files && writer_config.write_folders
                                                  ? "all"
                                                  : (writer_config.write_files ? "files" : "folders");
        if (writer_config.enabled) {
            writer_config.run_info =
                make_metadata_scan_run_info(source_root,
                                            recursive,
                                            to_string(writer_config.format),
                                            effective_records,
                                            report.meta_reader_threads,
                                            report.metadata_async_depth,
                                            report.record_buffer_slots,
                                            max_duration_seconds,
                                            report.stats_interval_seconds);
            report.scan_run_id = writer_config.run_info->run_id;
        }
        std::optional<MetadataRecordWriter> record_writer;
        std::unique_ptr<PartitionedMetadataWriter> partitioned_writer;
        const bool transport_discard = metadata_output_partition_mode == "transport-discard";
        const bool route_discard = metadata_output_partition_mode == "route-discard";
        if ((writer_config.enabled || transport_discard || route_discard) && report.metadata_output_partitions > 1U) {
            if (metadata_output_partition_mode != "processes") {
                if (!transport_discard && !route_discard) {
                    throw std::invalid_argument("partitioned metadata output requires --metadata-output-partition-mode processes");
                }
            }
            if (transport_discard && writer_config.output_path.empty()) {
                throw std::invalid_argument("transport-discard metadata partition mode requires --metadata-output for socket/report directory");
            }
        } else if (writer_config.enabled) {
            record_writer.emplace(writer_config);
        }
        const std::size_t flat_slots =
            record_buffer_slots == 0U
                ? kMetadataDiscardDefaultBufferSlots
                : std::max<std::size_t>(1U, record_buffer_slots);
        report.record_buffer_slots = flat_slots;

        RawBufferPool metadata_pool = make_metadata_batch_buffer_pool(flat_slots);
        if ((writer_config.enabled || transport_discard) && report.metadata_output_partitions > 1U) {
            partitioned_writer = std::make_unique<PartitionedMetadataWriter>(
                writer_config,
                writer_config.output_path,
                report.metadata_output_partitions,
                &metadata_pool,
                transport_discard);
        }
        RawBufferPool folder_pool = make_folder_work_buffer_pool(
            kFolderWorkBufferPoolId,
            folder_work_slots(report.meta_reader_threads, report.metadata_async_depth));
        RawBufferPool folder_feedback_pool = make_folder_work_buffer_pool(
            kFolderFeedbackBufferPoolId,
            folder_work_slots(report.meta_reader_threads, report.metadata_async_depth));
        BufferPoolRegistry registry;
        registry.register_pool(metadata_pool);
        BufQueue metadata_queue(flat_slots);
        BufQueue folder_queue(folder_pool.capacity());
        BufQueue folder_feedback_queue(folder_feedback_pool.capacity());
        FolderSeederJob folder_seeder(recursive,
                                      max_duration_seconds,
                                      folder_pool,
                                      folder_feedback_pool,
                                      folder_queue,
                                      folder_feedback_queue);
        const bool direct_partition_transport = partitioned_writer != nullptr && !route_discard;
        std::unique_ptr<NfsMetaReaderBufferJob> scanner;
        if (direct_partition_transport) {
            scanner = std::make_unique<NfsMetaReaderBufferJob>(source_root.string(),
                                                               "size",
                                                               report.meta_reader_threads,
                                                               report.metadata_async_depth,
                                                               folder_pool,
                                                               folder_feedback_pool,
                                                               folder_queue,
                                                               folder_feedback_queue,
                                                               metadata_pool,
                                                               partitioned_writer->route_queues(),
                                                               max_duration_seconds);
        } else {
            scanner = std::make_unique<NfsMetaReaderBufferJob>(source_root.string(),
                                                               "size",
                                                               report.meta_reader_threads,
                                                               report.metadata_async_depth,
                                                               folder_pool,
                                                               folder_feedback_pool,
                                                               folder_queue,
                                                               folder_feedback_queue,
                                                               metadata_pool,
                                                               metadata_queue,
                                                               max_duration_seconds);
        }
        const std::size_t metadata_consumer_threads =
            (partitioned_writer != nullptr || route_discard)
                ? std::min(report.meta_reader_threads, report.metadata_output_partitions)
                : 1U;
        std::vector<std::unique_ptr<BufQueue>> route_discard_queues;
        std::vector<std::unique_ptr<BufferDiscarderJob>> route_discarders;
        std::unique_ptr<ThreadedJob> metadata_consumer;
        if (direct_partition_transport) {
            // Scanner output goes directly to the partition sender queues.
        } else if (route_discard) {
            route_discard_queues.reserve(report.metadata_output_partitions);
            route_discarders.reserve(report.metadata_output_partitions);
            for (std::size_t index = 0; index < report.metadata_output_partitions; ++index) {
                route_discard_queues.push_back(std::make_unique<BufQueue>(kMetadataPartitionTransportPoolSlots));
                route_discarders.push_back(std::make_unique<BufferDiscarderJob>(
                    BufferDiscarderConfig(1U),
                    *route_discard_queues.back(),
                    registry));
            }
            metadata_consumer = std::make_unique<PartitionedFlatFolderMetadataRouterJob>(
                metadata_consumer_threads,
                metadata_queue,
                registry,
                &stats_discarder,
                route_discard_queues);
        } else if (partitioned_writer != nullptr) {
            metadata_consumer = std::make_unique<PartitionedFlatFolderMetadataRouterJob>(
                metadata_consumer_threads,
                metadata_queue,
                registry,
                &stats_discarder,
                *partitioned_writer);
        } else {
            metadata_consumer = std::make_unique<FlatFolderMetadataConsumerJob>(
                metadata_consumer_threads,
                metadata_queue,
                registry,
                &stats_discarder,
                record_writer.has_value() ? &*record_writer : nullptr,
                nullptr);
        }

        stats_discarder.start();
        stats_discarder.record_folder("");
        folder_seeder.start();
        for (auto& discarder : route_discarders) {
            discarder->start();
        }
        if (metadata_consumer) {
            metadata_consumer->start();
        }
        scanner->start();
        scanner->wait();
        folder_seeder.wait();
        if (auto error = folder_seeder.error()) {
            std::rethrow_exception(error);
        }
        if (metadata_consumer) {
            metadata_consumer->wait();
        }
        if (direct_partition_transport && partitioned_writer) {
            report.metadata_queue_shards = report.metadata_output_partitions;
            report.metadata_queue_capacity = partitioned_writer->route_queue_capacity();
            report.metadata_queue_high_watermark = partitioned_writer->route_queue_high_watermark();
            report.metadata_queue_full = partitioned_writer->route_queue_full();
        } else {
            report.metadata_queue_shards = 1U;
            report.metadata_queue_capacity = metadata_queue.capacity();
            report.metadata_queue_high_watermark = metadata_queue.high_watermark();
            report.metadata_queue_full = metadata_queue.high_watermark() >= metadata_queue.capacity();
        }
        for (auto& queue : route_discard_queues) {
            queue->close();
        }
        for (auto& discarder : route_discarders) {
            discarder->wait();
        }
        report.learned_meta_reader_threads = report.meta_reader_threads;
        stats_discarder.stop();
        if (direct_partition_transport) {
            const DistributedDiffRunReport scanner_stats = scanner->stats();
            report.files_seen = static_cast<std::size_t>(scanner_stats.files_compared);
            report.checker_discarded = report.files_seen;
            report.folders_found = static_cast<std::size_t>(scanner_stats.folders_sent);
            report.logical_size_bytes = scanner_stats.source_logical_size_bytes;
        } else {
            const MetadataStatsSnapshot stats = stats_discarder.snapshot();
            report.files_seen = stats.files_found;
            report.checker_discarded = stats.records_discarded;
            report.folders_found = stats.folders_found;
            report.logical_size_bytes = stats.logical_size_bytes;
            report.records_per_second = stats.records_per_second;
        }
        if (record_writer.has_value()) {
            record_writer->close();
            report.metadata_files_written = record_writer->files_written();
            report.metadata_folders_written = record_writer->folders_written();
        }
        if (partitioned_writer) {
            partitioned_writer->close();
            report.metadata_files_written = partitioned_writer->files_written();
            report.metadata_folders_written = partitioned_writer->folders_written();
        }
    } else {
        const std::size_t flat_slots =
            record_buffer_slots == 0U
                ? kMetadataDiscardDefaultBufferSlots
                : std::max<std::size_t>(1U, record_buffer_slots);
        report.record_buffer_slots = flat_slots;

        RawBufferPool metadata_pool = make_metadata_batch_buffer_pool(flat_slots);
        RawBufferPool folder_pool = make_folder_work_buffer_pool(
            kFolderWorkBufferPoolId,
            folder_work_slots(report.meta_reader_threads, report.metadata_async_depth));
        RawBufferPool folder_feedback_pool = make_folder_work_buffer_pool(
            kFolderFeedbackBufferPoolId,
            folder_work_slots(report.meta_reader_threads, report.metadata_async_depth));
        BufferPoolRegistry registry;
        registry.register_pool(metadata_pool);
        BufQueue metadata_to_discard(flat_slots);
        const std::size_t sharded_queue_shards =
            sharded_discard ? std::max<std::size_t>(1U, report.metadata_output_partitions) : 1U;
        const std::size_t sharded_queue_depth_per_shard =
            std::max<std::size_t>(1U, (flat_slots + sharded_queue_shards - 1U) / sharded_queue_shards);
        ShardedBufQueue sharded_metadata_to_discard(sharded_queue_shards, sharded_queue_depth_per_shard);
        BufQueue folder_queue(folder_pool.capacity());
        BufQueue folder_feedback_queue(folder_feedback_pool.capacity());

        FolderSeederJob folder_seeder(recursive,
                                      max_duration_seconds,
                                      folder_pool,
                                      folder_feedback_pool,
                                      folder_queue,
                                      folder_feedback_queue);
        report.metadata_queue_shards = sharded_discard ? sharded_queue_shards : 1U;
        report.metadata_queue_capacity =
            sharded_discard ? sharded_metadata_to_discard.capacity() : metadata_to_discard.capacity();
        std::unique_ptr<NfsMetaReaderBufferJob> scanner;
        std::unique_ptr<BufferDiscarderJob> discarder;
        if (sharded_discard) {
            scanner = std::make_unique<NfsMetaReaderBufferJob>(source_root.string(),
                                                               "size",
                                                               report.meta_reader_threads,
                                                               report.metadata_async_depth,
                                                               folder_pool,
                                                               folder_feedback_pool,
                                                               folder_queue,
                                                               folder_feedback_queue,
                                                               metadata_pool,
                                                               sharded_metadata_to_discard,
                                                               max_duration_seconds);
            discarder = std::make_unique<BufferDiscarderJob>(
                BufferDiscarderConfig(sharded_queue_shards),
                sharded_metadata_to_discard,
                registry);
        } else {
            scanner = std::make_unique<NfsMetaReaderBufferJob>(source_root.string(),
                                                               "size",
                                                               report.meta_reader_threads,
                                                               report.metadata_async_depth,
                                                               folder_pool,
                                                               folder_feedback_pool,
                                                               folder_queue,
                                                               folder_feedback_queue,
                                                               metadata_pool,
                                                               metadata_to_discard,
                                                               max_duration_seconds);
            discarder = std::make_unique<BufferDiscarderJob>(BufferDiscarderConfig(1U),
                                                             metadata_to_discard,
                                                             registry);
        }

        folder_seeder.start();
        discarder->start();
        scanner->start();
        scanner->wait();
        folder_seeder.wait();
        if (auto error = folder_seeder.error()) {
            std::rethrow_exception(error);
        }
        discarder->wait();

        report.metadata_queue_high_watermark = sharded_discard ? sharded_metadata_to_discard.high_watermark()
                                                               : metadata_to_discard.high_watermark();
        report.metadata_queue_full = report.metadata_queue_high_watermark >= report.metadata_queue_capacity;
        const DistributedDiffRunReport scanner_stats = scanner->stats();
        report.files_seen = static_cast<std::size_t>(scanner_stats.files_compared);
        report.checker_emitted = 0;
        report.checker_discarded = report.files_seen;
        report.folders_found = static_cast<std::size_t>(scanner_stats.folders_sent);
        report.logical_size_bytes = scanner_stats.source_logical_size_bytes;
        report.learned_meta_reader_threads = report.meta_reader_threads;
    }

    report.elapsed_seconds =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    if (report.records_per_second == 0.0 && report.elapsed_seconds > 0.0) {
        report.records_per_second = static_cast<double>(report.files_seen) / report.elapsed_seconds;
    }
    return report;
}

DataReadBenchmarkReport TransferEngine::benchmark_data_read_pipeline(const std::filesystem::path& source_root,
                                                                     bool recursive,
                                                                     std::size_t meta_reader_threads,
                                                                     std::size_t metadata_async_depth,
                                                                     std::size_t readdirplus_page_bytes,
                                                                     std::size_t data_reader_threads,
                                                                     std::size_t data_outstanding_requests,
                                                                     std::size_t small_file_async_window,
                                                                     bool split_small_large,
                                                                     bool dual_scan_small_large,
                                                                     bool recon_scan_enabled,
                                                                     bool morph_large_readers_to_small,
                                                                     bool bucket_priority_enabled,
                                                                     std::uint64_t split_small_file_threshold,
                                                                     std::size_t recon_meta_reader_threads,
                                                                     std::size_t recon_metadata_async_depth,
                                                                     std::uint64_t recon_page_sleep_us,
                                                                     std::size_t small_meta_reader_threads,
                                                                     std::size_t large_meta_reader_threads,
                                                                     std::size_t small_data_reader_threads,
                                                                     std::size_t large_data_reader_threads,
                                                                     std::size_t large_data_outstanding_requests,
                                                                     bool pipeline_autoscale,
                                                                     bool large_reader_autoscale,
                                                                     std::size_t large_reader_initial_threads,
                                                                     std::uint64_t autoscale_interval_ms,
                                                                     std::string autoscale_profile,
                                                                     std::filesystem::path autoscale_settings_path,
                                                                     std::uint64_t max_file_size_bytes,
                                                                     std::size_t max_files_queued,
                                                                     std::size_t small_max_files_queued,
                                                                     std::size_t large_max_files_queued,
                                                                     std::size_t data_buffer_slots,
                                                                     std::size_t data_queue_depth,
                                                                     const std::string& data_copy_mode,
                                                                     bool pack_small_files,
                                                                     double max_duration_seconds,
                                                                     std::uint32_t stats_interval_seconds,
                                                                     const std::filesystem::path& status_socket_path) const {
    NfsMetaReaderConfig meta_config = load_nfs_meta_reader_config(config_store_);
    meta_config.source_root = source_root.string();
    meta_config.recursive = recursive;
    if (meta_reader_threads != 0U) {
        meta_config.worker_count = meta_reader_threads;
        meta_config.thread_count = meta_reader_threads;
    }
    if (metadata_async_depth != 0U) {
        meta_config.async_directory_depth = metadata_async_depth;
    }
    if (readdirplus_page_bytes != 0U) {
        meta_config.readdirplus_page_bytes = readdirplus_page_bytes;
    }

    NfsDataReaderConfig data_config = load_nfs_data_reader_config(config_store_);
    data_config.source_root = source_root.string();
    if (data_reader_threads != 0) {
        data_config.data_reader_worker_count = data_reader_threads;
    }
    if (data_outstanding_requests != 0) {
        data_config.outstanding_requests = data_outstanding_requests;
    }
    if (small_file_async_window != 0) {
        data_config.small_file_async_window = small_file_async_window;
    }
    if (!data_copy_mode.empty()) {
        data_config.copy_data_from_nfs = parse_data_copy_mode(data_copy_mode);
    }
    data_config.pack_small_files = pack_small_files;

    NfsMetaReader meta_reader(meta_config);
    NfsDataReader data_reader(data_config);

    DataReadBenchmarkReport report;
    report.meta_reader_async = meta_reader.using_async_backend();
    report.data_reader_async = data_reader.using_async_backend();
    report.meta_reader_threads = std::max<std::size_t>(1, meta_config.worker_count);
    report.metadata_async_depth = std::max<std::size_t>(1, meta_config.async_directory_depth);
    report.readdirplus_page_bytes = meta_config.readdirplus_page_bytes;
    report.data_reader_threads = std::max<std::size_t>(1, data_config.data_reader_worker_count);
    report.data_outstanding_requests = std::max<std::size_t>(1, data_config.outstanding_requests);
    report.small_file_async_window = data_config.small_file_async_window != 0U
                                         ? data_config.small_file_async_window
                                         : report.data_outstanding_requests;
    report.split_small_large = split_small_large || dual_scan_small_large;
    report.dual_scan_small_large = dual_scan_small_large;
    report.recon_scan_enabled = recon_scan_enabled;
    report.morph_large_readers_to_small = morph_large_readers_to_small;
    report.bucket_priority_enabled = bucket_priority_enabled;
    report.split_small_file_threshold = split_small_file_threshold == 0U ? config_.small_file_threshold
                                                                         : split_small_file_threshold;
    report.small_meta_reader_threads =
        small_meta_reader_threads == 0U ? report.meta_reader_threads : small_meta_reader_threads;
    report.large_meta_reader_threads =
        large_meta_reader_threads == 0U ? report.meta_reader_threads : large_meta_reader_threads;
    const std::size_t hardware_threads =
        std::max<std::size_t>(1, std::thread::hardware_concurrency());
    const std::size_t default_recon_threads =
        report.bucket_priority_enabled
            ? std::max<std::size_t>(report.small_meta_reader_threads,
                                    std::min<std::size_t>(256U, hardware_threads * 2U))
            : std::size_t {1};
    report.recon_meta_reader_threads =
        recon_meta_reader_threads == 0U ? default_recon_threads : recon_meta_reader_threads;
    report.recon_metadata_async_depth =
        recon_metadata_async_depth == 0U
            ? (report.bucket_priority_enabled ? report.metadata_async_depth : std::size_t {1})
            : recon_metadata_async_depth;
    report.recon_page_sleep_us = recon_page_sleep_us;
    const bool data_pipeline_autoscale = report.split_small_large && (pipeline_autoscale || large_reader_autoscale);
    const std::size_t auto_worker_capacity = default_autoscale_max_workers();
    report.small_data_reader_threads =
        small_data_reader_threads == 0U
            ? (data_pipeline_autoscale ? auto_worker_capacity : report.data_reader_threads)
            : small_data_reader_threads;
    report.large_data_reader_threads =
        large_data_reader_threads == 0U
            ? (data_pipeline_autoscale ? auto_worker_capacity : std::max<std::size_t>(1, report.data_reader_threads / 2U))
            : large_data_reader_threads;
    report.large_data_outstanding_requests =
        large_data_outstanding_requests == 0U ? report.data_outstanding_requests
                                              : large_data_outstanding_requests;
    report.pipeline_autoscale = pipeline_autoscale;
    report.large_reader_autoscale = large_reader_autoscale;
    report.large_reader_initial_threads = large_reader_initial_threads == 0U
                                              ? std::max<std::size_t>(1, report.large_data_reader_threads / 2U)
                                              : large_reader_initial_threads;
    report.autoscale_interval_ms = autoscale_interval_ms;
    report.autoscale_profile = autoscale_profile.empty() ? "benchmark_data_split_small_large"
                                                         : std::move(autoscale_profile);
    report.autoscale_settings_path =
        autoscale_settings_path.empty() ? default_autoscale_settings_path()
                                        : std::move(autoscale_settings_path);
    report.max_file_size_bytes = max_file_size_bytes;
    report.max_files_queued = std::max<std::size_t>(1, max_files_queued);
    report.small_max_files_queued =
        std::max<std::size_t>(1, small_max_files_queued == 0U ? report.max_files_queued : small_max_files_queued);
    report.large_max_files_queued =
        std::max<std::size_t>(1, large_max_files_queued == 0U ? report.max_files_queued : large_max_files_queued);
    report.data_copy_mode = data_copy_mode_name(data_config.copy_data_from_nfs);
    report.pack_small_files = data_config.pack_small_files;

    DataReadBenchmarkSnapshot snapshot;
    if (report.split_small_large) {
        NfsDataReaderConfig small_data_config = data_config;
        NfsDataReaderConfig large_data_config = data_config;
        small_data_config.data_reader_worker_count = report.small_data_reader_threads;
        small_data_config.outstanding_requests = 1U;
        small_data_config.small_file_async_window = report.small_file_async_window;
        small_data_config.pack_small_files = data_config.pack_small_files;
        large_data_config.data_reader_worker_count = report.large_data_reader_threads;
        large_data_config.outstanding_requests = report.large_data_outstanding_requests;
        snapshot = run_parallel_split_data_read_scan(meta_config,
                                                     small_data_config,
                                                     large_data_config,
                                                     report.split_small_file_threshold,
                                                     report.max_files_queued,
                                                     report.small_max_files_queued,
                                                     report.large_max_files_queued,
                                                     report.dual_scan_small_large,
                                                     report.recon_scan_enabled,
                                                     report.recon_meta_reader_threads,
                                                     report.recon_metadata_async_depth,
                                                     report.recon_page_sleep_us,
                                                     report.morph_large_readers_to_small,
                                                     report.bucket_priority_enabled,
                                                     report.small_meta_reader_threads,
                                                     report.large_meta_reader_threads,
                                                     data_buffer_slots,
                                                     data_queue_depth,
                                                     max_duration_seconds,
                                                     stats_interval_seconds,
                                                     SplitDataReadAutoscaleConfig {
                                                         pipeline_autoscale,
                                                         large_reader_autoscale,
                                                         large_reader_initial_threads,
                                                         std::size_t {1},
                                                         autoscale_interval_ms,
                                                         report.autoscale_profile,
                                                         report.autoscale_settings_path,
                                                     },
                                                     status_socket_path);
    } else {
        snapshot = run_parallel_data_read_scan(meta_config,
                                               data_config,
                                               max_file_size_bytes,
                                               report.max_files_queued,
                                               data_buffer_slots,
                                               data_queue_depth,
                                               max_duration_seconds,
                                               stats_interval_seconds,
                                               status_socket_path);
    }
    report.files_found = snapshot.files_found;
    report.folders_found = snapshot.folders_found;
    report.files_read = snapshot.files_read;
    report.files_failed = snapshot.files_failed;
    report.logical_size_bytes = snapshot.logical_size_bytes;
    report.bytes_read = snapshot.bytes_read;
    report.small_files_found = snapshot.small_files_found;
    report.large_files_found = snapshot.large_files_found;
    report.small_files_read = snapshot.small_files_read;
    report.large_files_read = snapshot.large_files_read;
    report.small_bytes_read = snapshot.small_bytes_read;
    report.large_bytes_read = snapshot.large_bytes_read;
    report.small_logical_size_bytes = snapshot.small_logical_size_bytes;
    report.large_logical_size_bytes = snapshot.large_logical_size_bytes;
    report.recon_files_found = snapshot.recon_files_found;
    report.recon_folders_found = snapshot.recon_folders_found;
    report.recon_small_files_found = snapshot.recon_small_files_found;
    report.recon_large_files_found = snapshot.recon_large_files_found;
    report.recon_logical_size_bytes = snapshot.recon_logical_size_bytes;
    report.recon_small_logical_size_bytes = snapshot.recon_small_logical_size_bytes;
    report.recon_large_logical_size_bytes = snapshot.recon_large_logical_size_bytes;
    report.recon_completed = snapshot.recon_completed;
    report.bytes_per_second = snapshot.bytes_per_second;
    report.gigabits_per_second = snapshot.gigabits_per_second;
    report.files_per_second = snapshot.files_per_second;
    report.small_files_per_second = snapshot.small_files_per_second;
    report.large_files_per_second = snapshot.large_files_per_second;
    report.small_gigabits_per_second = snapshot.small_gigabits_per_second;
    report.large_gigabits_per_second = snapshot.large_gigabits_per_second;
    report.elapsed_seconds = snapshot.elapsed_seconds;
    report.data_buffer_slots = snapshot.data_buffer_slots;
    report.data_queue_depth = snapshot.data_queue_depth;
    const NfsAsyncReadLatencySnapshot async_read_latency = snapshot_nfs_async_read_latency_metrics();
    report.async_read_queued = async_read_latency.queued;
    report.async_read_completed = async_read_latency.completed;
    report.async_read_short = async_read_latency.short_reads;
    report.async_read_failed = async_read_latency.failed;
    report.async_read_zero = async_read_latency.zero_reads;
    report.async_read_bytes_requested = async_read_latency.bytes_requested;
    report.async_read_bytes_completed = async_read_latency.bytes_completed;
    report.async_read_avg_latency_ms = async_read_latency.completed != 0U
                                           ? static_cast<double>(async_read_latency.latency_ns) /
                                                 static_cast<double>(async_read_latency.completed) / 1'000'000.0
                                           : 0.0;
    report.async_read_max_latency_ms = static_cast<double>(async_read_latency.max_latency_ns) / 1'000'000.0;
    const NfsAsyncCommandLatencySnapshot command_latency = snapshot_nfs_async_command_latency_metrics();
    report.async_open_completed = command_latency.open_completed;
    report.async_open_failed = command_latency.open_failed;
    report.async_open_avg_latency_ms = command_latency.open_completed != 0U
                                           ? static_cast<double>(command_latency.open_latency_ns) /
                                                 static_cast<double>(command_latency.open_completed) / 1'000'000.0
                                           : 0.0;
    report.async_open_max_latency_ms = static_cast<double>(command_latency.open_max_latency_ns) / 1'000'000.0;
    report.async_close_completed = command_latency.close_completed;
    report.async_close_failed = command_latency.close_failed;
    report.async_close_avg_latency_ms = command_latency.close_completed != 0U
                                            ? static_cast<double>(command_latency.close_latency_ns) /
                                                  static_cast<double>(command_latency.close_completed) / 1'000'000.0
                                            : 0.0;
    report.async_close_max_latency_ms = static_cast<double>(command_latency.close_max_latency_ns) / 1'000'000.0;
    return report;
}

DataReadBenchmarkReport TransferEngine::benchmark_data_write_pipeline(const std::filesystem::path& source_root,
                                                                      const std::string& target_root,
                                                                      bool recursive,
                                                                      std::size_t meta_reader_threads,
                                                                      std::size_t metadata_async_depth,
                                                                      std::size_t readdirplus_page_bytes,
                                                                      std::size_t data_reader_threads,
                                                                      std::size_t data_writer_threads,
                                                                      std::size_t data_writer_async_window,
                                                                      std::size_t data_outstanding_requests,
                                                                      std::size_t small_file_async_window,
                                                                      std::uint64_t min_file_size_bytes,
                                                                      std::uint64_t max_file_size_bytes,
                                                                      std::size_t max_files_queued,
                                                                      std::size_t data_buffer_slots,
                                                                      std::size_t data_queue_depth,
                                                                      const std::string& data_copy_mode,
                                                                      bool pack_small_files,
                                                                      double max_duration_seconds,
                                                                      std::uint32_t stats_interval_seconds,
                                                                      bool verify_hash,
                                                                      bool preserve_target_metadata,
                                                                      bool target_fsync,
                                                                      bool ensure_target_directories,
                                                                      bool stable_small_file_writes,
                                                                      bool tcp_cork_small_file_writes,
                                                                      bool direct_reactor_writes,
                                                                      bool direct_reactor_submit,
                                                                      std::size_t data_writer_reactors,
                                                                      std::size_t reactors_per_ip,
                                                                      std::size_t data_writer_file_window,
                                                                      bool mkdir_only,
                                                                      bool folder_ready_discard,
                                                                      bool folder_ready_write,
                                                                      bool folder_ready_mixed_write,
                                                                      std::string small_file_target_ips,
                                                                      std::string large_file_target_ips) const {
    NfsMetaReaderConfig meta_config = load_nfs_meta_reader_config(config_store_);
    meta_config.source_root = source_root.string();
    meta_config.recursive = recursive;
    if (meta_reader_threads != 0U) {
        meta_config.worker_count = meta_reader_threads;
        meta_config.thread_count = meta_reader_threads;
    }
    if (metadata_async_depth != 0U) {
        meta_config.async_directory_depth = metadata_async_depth;
    }
    if (readdirplus_page_bytes != 0U) {
        meta_config.readdirplus_page_bytes = readdirplus_page_bytes;
    }

    NfsDataReaderConfig data_config = load_nfs_data_reader_config(config_store_);
    data_config.source_root = source_root.string();
    if (data_reader_threads != 0U) {
        data_config.data_reader_worker_count = data_reader_threads;
    }
    if (data_outstanding_requests != 0U) {
        data_config.outstanding_requests = data_outstanding_requests;
    }
    if (small_file_async_window != 0U) {
        data_config.small_file_async_window = small_file_async_window;
    }
    if (!data_copy_mode.empty()) {
        data_config.copy_data_from_nfs = parse_data_copy_mode(data_copy_mode);
    }
    data_config.pack_small_files = pack_small_files;

    TargetDataWriterConfig writer_config = load_target_data_writer_config(config_store_);
    writer_config.target_root = target_root;
    writer_config.verify_hash = verify_hash;
    writer_config.preserve_metadata = preserve_target_metadata;
    writer_config.fsync_on_finish = target_fsync;
    writer_config.ensure_parent_directories = ensure_target_directories;
    writer_config.stable_small_file_writes = stable_small_file_writes;
    writer_config.tcp_cork_small_file_writes = tcp_cork_small_file_writes;
    writer_config.direct_reactor_writes = direct_reactor_writes;
    writer_config.direct_reactor_submit = direct_reactor_submit;
    writer_config.reactor_count = data_writer_reactors;
    writer_config.reactors_per_ip = std::max<std::size_t>(1U, reactors_per_ip);
    const bool folder_ready_payload_write = folder_ready_write || folder_ready_mixed_write;
    const bool folder_ready_nfs_packed_write =
        folder_ready_payload_write && is_nfs_url(target_root) && data_config.pack_small_files;
    if (folder_ready_nfs_packed_write && !direct_reactor_writes) {
        writer_config.direct_reactor_submit = true;
    }
    if ((folder_ready_write || folder_ready_mixed_write) && writer_config.direct_reactor_submit &&
        writer_config.reactor_count == 0U) {
        writer_config.reactor_count = 64U;
    }
    if ((folder_ready_write || folder_ready_mixed_write) && writer_config.direct_reactor_submit &&
        data_writer_file_window == 0U) {
        writer_config.max_concurrent_file_transactions = 64U;
    }
    if (data_writer_file_window != 0U) {
        writer_config.max_concurrent_file_transactions = data_writer_file_window;
    }
    if (data_writer_threads != 0U) {
        writer_config.worker_count = data_writer_threads;
    } else if (folder_ready_payload_write) {
        writer_config.worker_count = 8U;
    }
    if (data_writer_async_window != 0U) {
        writer_config.async_window = data_writer_async_window;
    }

    NfsMetaReader meta_reader(meta_config);
    NfsDataReader data_reader(data_config);

    DataReadBenchmarkReport report;
    report.meta_reader_async = meta_reader.using_async_backend();
    report.data_reader_async = (mkdir_only || folder_ready_discard) ? false : data_reader.using_async_backend();
    report.meta_reader_threads = std::max<std::size_t>(1, meta_config.worker_count);
    report.metadata_async_depth = std::max<std::size_t>(1, meta_config.async_directory_depth);
    report.readdirplus_page_bytes = meta_config.readdirplus_page_bytes;
    report.data_reader_threads = (mkdir_only || folder_ready_discard)
                                     ? 0U
                                     : std::max<std::size_t>(1, data_config.data_reader_worker_count);
    report.data_writer_threads = writer_config.direct_reactor_submit ? 0U
                                                                      : target_data_writer_effective_worker_count(writer_config);
    report.data_outstanding_requests = std::max<std::size_t>(1, data_config.outstanding_requests);
    report.small_file_async_window = data_config.small_file_async_window != 0U
                                         ? data_config.small_file_async_window
                                         : report.data_outstanding_requests;
    report.min_file_size_bytes = min_file_size_bytes;
    report.max_file_size_bytes = max_file_size_bytes;
    report.data_writer_async_window = std::max<std::size_t>(1, writer_config.async_window);
    report.max_files_queued = std::max<std::size_t>(1, max_files_queued);
    report.data_copy_mode = data_copy_mode_name(data_config.copy_data_from_nfs);
    report.pack_small_files = data_config.pack_small_files;
    report.target_root = target_root;
    report.data_writer_file_window = writer_config.max_concurrent_file_transactions;
    report.data_writer_reactors = writer_config.reactor_count != 0U
                                      ? writer_config.reactor_count
                                      : expand_nfs_url_server_candidates(target_root).size() *
                                            std::max<std::size_t>(1U, writer_config.reactors_per_ip);

    TargetWriterStats writer_stats;
    std::size_t queue_capacity = 0;
    std::size_t queue_high_watermark = 0;
    DataReadBenchmarkSnapshot snapshot;
    if (mkdir_only) {
        TargetMetaWriterConfig meta_writer_config(
            data_writer_threads == 0U ? target_data_writer_effective_worker_count(writer_config)
                                      : data_writer_threads,
            target_root);
        meta_writer_config.preserve_metadata = false;
        if (data_writer_file_window != 0U) {
            meta_writer_config.async_window = data_writer_file_window;
        }
        snapshot = run_parallel_mkdir_only_scan(meta_config,
                                                meta_writer_config,
                                                report.max_files_queued,
                                                max_duration_seconds,
                                                writer_stats,
                                                queue_capacity,
                                                queue_high_watermark);
    } else if (folder_ready_discard) {
        snapshot = run_parallel_folder_ready_discard_scan(meta_config,
                                                          writer_config,
                                                          data_reader_threads == 0U ? data_config.data_reader_worker_count
                                                                                   : data_reader_threads,
                                                          min_file_size_bytes,
                                                          max_file_size_bytes,
                                                          report.max_files_queued,
                                                          max_duration_seconds,
                                                          stats_interval_seconds,
                                                          writer_stats,
                                                          queue_capacity,
                                                          queue_high_watermark);
    } else if (folder_ready_mixed_write) {
        snapshot = run_parallel_folder_ready_mixed_write_scan(meta_config,
                                                              data_config,
                                                              writer_config,
                                                              max_file_size_bytes == 0U ? (1024U * 1024U - 1U)
                                                                                        : max_file_size_bytes,
                                                              report.max_files_queued,
                                                              data_buffer_slots,
                                                              data_queue_depth,
                                                              max_duration_seconds,
                                                              stats_interval_seconds,
                                                              writer_stats,
                                                              queue_capacity,
                                                              queue_high_watermark,
                                                              small_file_target_ips,
                                                              large_file_target_ips);
    } else if (folder_ready_write) {
        snapshot = run_parallel_folder_ready_write_scan(meta_config,
                                                        data_config,
                                                        writer_config,
                                                        min_file_size_bytes,
                                                        max_file_size_bytes,
                                                        report.max_files_queued,
                                                        data_buffer_slots,
                                                        data_queue_depth,
                                                        max_duration_seconds,
                                                        stats_interval_seconds,
                                                        writer_stats,
                                                        queue_capacity,
                                                        queue_high_watermark);
    } else {
        snapshot = run_parallel_data_write_scan(meta_config,
                                                data_config,
                                                writer_config,
                                                min_file_size_bytes,
                                                max_file_size_bytes,
                                                report.max_files_queued,
                                                data_buffer_slots,
                                                data_queue_depth,
                                                max_duration_seconds,
                                                stats_interval_seconds,
                                                writer_stats,
                                                queue_capacity,
                                                queue_high_watermark);
    }
    report.files_found = snapshot.files_found;
    report.folders_found = snapshot.folders_found;
    report.files_read = snapshot.files_read;
    report.files_failed = snapshot.files_failed;
    report.logical_size_bytes = snapshot.logical_size_bytes;
    report.bytes_read = snapshot.bytes_read;
    report.bytes_per_second = snapshot.bytes_per_second;
    report.gigabits_per_second = snapshot.gigabits_per_second;
    report.files_per_second = snapshot.files_per_second;
    report.elapsed_seconds = snapshot.elapsed_seconds;
    report.data_buffer_slots = snapshot.data_buffer_slots;
    report.data_queue_depth = snapshot.data_queue_depth;
    report.files_written = static_cast<std::size_t>(writer_stats.files_written);
    report.write_failed = static_cast<std::size_t>(writer_stats.files_failed);
    report.folders_written = static_cast<std::size_t>(writer_stats.folders_written);
    report.folders_per_second = snapshot.folders_per_second;
    report.bytes_written = writer_stats.bytes_written;
    report.small_files_found = snapshot.small_files_found;
    report.large_files_found = snapshot.large_files_found;
    report.small_files_read = snapshot.small_files_read;
    report.large_files_read = snapshot.large_files_read;
    report.small_bytes_read = snapshot.small_bytes_read;
    report.large_bytes_read = snapshot.large_bytes_read;
    report.small_files_per_second = snapshot.small_files_per_second;
    report.large_files_per_second = snapshot.large_files_per_second;
    report.small_gigabits_per_second = snapshot.small_gigabits_per_second;
    report.large_gigabits_per_second = snapshot.large_gigabits_per_second;
    report.data_queue_shards = (mkdir_only || folder_ready_discard || folder_ready_write || folder_ready_mixed_write)
                                   ? 1U
                                   : (writer_config.direct_reactor_submit ? 0U : report.data_writer_threads);
    report.data_queue_capacity = queue_capacity;
    report.data_queue_high_watermark = queue_high_watermark;

    const NfsAsyncReadLatencySnapshot async_read_latency = snapshot_nfs_async_read_latency_metrics();
    report.async_read_queued = async_read_latency.queued;
    report.async_read_completed = async_read_latency.completed;
    report.async_read_short = async_read_latency.short_reads;
    report.async_read_failed = async_read_latency.failed;
    report.async_read_zero = async_read_latency.zero_reads;
    report.async_read_bytes_requested = async_read_latency.bytes_requested;
    report.async_read_bytes_completed = async_read_latency.bytes_completed;
    report.async_read_avg_latency_ms = async_read_latency.completed != 0U
                                           ? static_cast<double>(async_read_latency.latency_ns) /
                                                 static_cast<double>(async_read_latency.completed) / 1'000'000.0
                                           : 0.0;
    report.async_read_max_latency_ms = static_cast<double>(async_read_latency.max_latency_ns) / 1'000'000.0;
    return report;
}

DataReadBenchmarkReport TransferEngine::benchmark_nfs_open_pipeline(const std::filesystem::path& source_root,
                                                                    bool recursive,
                                                                    std::size_t meta_reader_threads,
                                                                    std::size_t metadata_async_depth,
                                                                    std::size_t open_threads,
                                                                    std::size_t max_files_queued,
                                                                    double max_duration_seconds,
                                                                    std::uint32_t stats_interval_seconds) const {
    NfsMetaReaderConfig meta_config = load_nfs_meta_reader_config(config_store_);
    meta_config.source_root = source_root.string();
    meta_config.recursive = recursive;
    if (meta_reader_threads != 0U) {
        meta_config.worker_count = meta_reader_threads;
        meta_config.thread_count = meta_reader_threads;
    }
    if (metadata_async_depth != 0U) {
        meta_config.async_directory_depth = metadata_async_depth;
    }

    NfsDataReaderConfig data_config = load_nfs_data_reader_config(config_store_);
    data_config.source_root = source_root.string();
    if (open_threads != 0U) {
        data_config.data_reader_worker_count = open_threads;
    }

    NfsMetaReader meta_reader(meta_config);
    NfsDataReader data_reader(data_config);

    DataReadBenchmarkReport report;
    report.meta_reader_async = meta_reader.using_async_backend();
    report.data_reader_async = data_reader.using_async_backend();
    report.meta_reader_threads = std::max<std::size_t>(1, meta_config.worker_count);
    report.metadata_async_depth = std::max<std::size_t>(1, meta_config.async_directory_depth);
    report.data_reader_threads = std::max<std::size_t>(1, data_config.data_reader_worker_count);
    report.max_files_queued = std::max<std::size_t>(1, max_files_queued);

    const DataReadBenchmarkSnapshot snapshot =
        run_parallel_nfs_open_scan(meta_config,
                                   data_config,
                                   report.max_files_queued,
                                   max_duration_seconds,
                                   stats_interval_seconds);
    report.files_found = snapshot.files_found;
    report.folders_found = snapshot.folders_found;
    report.files_read = snapshot.files_read;
    report.files_failed = snapshot.files_failed;
    report.logical_size_bytes = snapshot.logical_size_bytes;
    report.elapsed_seconds = snapshot.elapsed_seconds;

    const double files_per_second =
        snapshot.elapsed_seconds > 0.0 ? static_cast<double>(snapshot.files_read) / snapshot.elapsed_seconds : 0.0;
    report.bytes_per_second = files_per_second;
    report.gigabits_per_second = 0.0;

    const NfsAsyncCommandLatencySnapshot command_latency = snapshot_nfs_async_command_latency_metrics();
    report.async_open_completed = command_latency.open_completed;
    report.async_open_failed = command_latency.open_failed;
    report.async_open_avg_latency_ms = command_latency.open_completed != 0U
                                           ? static_cast<double>(command_latency.open_latency_ns) /
                                                 static_cast<double>(command_latency.open_completed) / 1'000'000.0
                                           : 0.0;
    report.async_open_max_latency_ms = static_cast<double>(command_latency.open_max_latency_ns) / 1'000'000.0;
    report.async_close_completed = command_latency.close_completed;
    report.async_close_failed = command_latency.close_failed;
    report.async_close_avg_latency_ms = command_latency.close_completed != 0U
                                            ? static_cast<double>(command_latency.close_latency_ns) /
                                                  static_cast<double>(command_latency.close_completed) / 1'000'000.0
                                            : 0.0;
    report.async_close_max_latency_ms = static_cast<double>(command_latency.close_max_latency_ns) / 1'000'000.0;
    return report;
}

DataHashBenchmarkReport TransferEngine::benchmark_data_hash_pipeline(const std::filesystem::path& source_root,
                                                                     bool recursive,
                                                                     const std::string& hash_algorithm,
                                                                     std::size_t meta_reader_threads,
                                                                     std::size_t metadata_async_depth,
                                                                     std::size_t data_reader_threads,
                                                                     std::size_t data_outstanding_requests,
                                                                     std::size_t small_file_async_window,
                                                                     std::size_t hash_worker_threads,
                                                                     std::size_t max_files_queued,
                                                                     std::size_t data_buffer_slots,
                                                                     std::size_t data_queue_depth,
                                                                     std::size_t hash_work_factor,
                                                                     bool pack_small_files,
                                                                     double max_duration_seconds,
                                                                     std::uint32_t stats_interval_seconds,
                                                                     const std::filesystem::path& status_socket_path) const {
    const DataHasherConfig hasher_config = load_data_hasher_config(config_store_);
    const ContentHashAlgorithm algorithm =
        hash_algorithm.empty() ? hasher_config.algorithm : parse_content_hash_algorithm(hash_algorithm);

    NfsMetaReaderConfig meta_config = load_nfs_meta_reader_config(config_store_);
    meta_config.source_root = source_root.string();
    meta_config.recursive = recursive;
    if (meta_reader_threads != 0U) {
        meta_config.worker_count = meta_reader_threads;
        meta_config.thread_count = meta_reader_threads;
    }
    if (metadata_async_depth != 0U) {
        meta_config.async_directory_depth = metadata_async_depth;
    }

    NfsDataReaderConfig data_config = load_nfs_data_reader_config(config_store_);
    data_config.source_root = source_root.string();
    if (data_reader_threads != 0) {
        data_config.data_reader_worker_count = data_reader_threads;
    }
    if (data_outstanding_requests != 0) {
        data_config.outstanding_requests = data_outstanding_requests;
    }
    if (small_file_async_window != 0) {
        data_config.small_file_async_window = small_file_async_window;
    }
    data_config.copy_data_from_nfs = true;
    data_config.pack_small_files = pack_small_files;

    NfsMetaReader meta_reader(meta_config);
    NfsDataReader data_reader(data_config);

    DataHashBenchmarkReport report =
        run_parallel_data_hash_scan(meta_config,
                                    data_config,
                                    algorithm,
                                    hash_worker_threads == 0 ? hasher_config.worker_count : hash_worker_threads,
                                    hash_work_factor == 0 ? hasher_config.work_factor : hash_work_factor,
                                    max_files_queued,
                                    data_buffer_slots,
                                    data_queue_depth,
                                    max_duration_seconds,
                                    stats_interval_seconds,
                                    status_socket_path);
    report.meta_reader_async = meta_reader.using_async_backend();
    report.data_reader_async = data_reader.using_async_backend();
    report.meta_reader_threads = std::max<std::size_t>(1, meta_config.worker_count);
    report.metadata_async_depth = std::max<std::size_t>(1, meta_config.async_directory_depth);
    report.data_reader_threads = std::max<std::size_t>(1, data_config.data_reader_worker_count);
    report.data_outstanding_requests = std::max<std::size_t>(1, data_config.outstanding_requests);
    report.small_file_async_window = data_config.small_file_async_window != 0U
                                         ? data_config.small_file_async_window
                                         : report.data_outstanding_requests;
    report.hash_worker_threads = hash_worker_threads == 0
                                     ? std::max<std::size_t>(1, hasher_config.worker_count)
                                     : std::max<std::size_t>(1, hash_worker_threads);
    report.max_files_queued = std::max<std::size_t>(1, max_files_queued);
    report.hash_algorithm = to_string(algorithm);
    report.hash_work_factor =
        std::max<std::size_t>(1, hash_work_factor == 0 ? hasher_config.work_factor : hash_work_factor);
    report.pack_small_files = data_config.pack_small_files;
    return report;
}

HashInventoryReport TransferEngine::hash_inventory_pipeline(const std::filesystem::path& source_root,
                                                            bool recursive,
                                                            const std::string& hash_algorithm,
                                                            std::size_t meta_reader_threads,
                                                            std::size_t metadata_async_depth,
                                                            std::size_t data_reader_threads,
                                                            std::size_t data_outstanding_requests,
                                                            std::size_t hash_worker_threads,
                                                            std::size_t max_files_queued,
                                                            std::size_t max_hash_chunks_queued,
                                                            const std::string& hash_mode_value,
                                                            std::uint64_t hash_block_size,
                                                            const std::filesystem::path& metadata_output_path,
                                                            const std::string& metadata_output_format,
                                                            const std::string& metadata_records,
                                                            double max_duration_seconds) const {
    const DataHasherConfig hasher_config = load_data_hasher_config(config_store_);
    const ContentHashAlgorithm algorithm =
        hash_algorithm.empty() ? hasher_config.algorithm : parse_content_hash_algorithm(hash_algorithm);
    const HashMode hash_mode = parse_hash_mode(hash_mode_value);

    NfsMetaReaderConfig meta_config = load_nfs_meta_reader_config(config_store_);
    meta_config.source_root = source_root.string();
    meta_config.recursive = recursive;
    if (meta_reader_threads != 0U) {
        meta_config.worker_count = meta_reader_threads;
        meta_config.thread_count = meta_reader_threads;
    }
    if (metadata_async_depth != 0U) {
        meta_config.async_directory_depth = metadata_async_depth;
    }

    NfsDataReaderConfig data_config = load_nfs_data_reader_config(config_store_);
    data_config.source_root = source_root.string();
    if (data_reader_threads != 0) {
        data_config.data_reader_worker_count = data_reader_threads;
    }
    if (data_outstanding_requests != 0) {
        data_config.outstanding_requests = data_outstanding_requests;
    }

    MetadataRecordWriterConfig writer_config = load_metadata_record_writer_config(config_store_);
    writer_config.enabled = true;
    if (!metadata_output_path.empty()) {
        writer_config.output_path = metadata_output_path;
        if (metadata_output_format.empty()) {
            writer_config.format = infer_metadata_record_format(metadata_output_path);
        }
    }
    if (!metadata_output_format.empty()) {
        writer_config.format = parse_metadata_record_format(metadata_output_format);
    }
    if (metadata_records == "all" || metadata_records.empty()) {
        writer_config.write_files = true;
        writer_config.write_folders = true;
    } else if (metadata_records == "files") {
        writer_config.write_files = true;
        writer_config.write_folders = false;
    } else if (metadata_records == "folders") {
        writer_config.write_files = false;
        writer_config.write_folders = true;
    } else {
        throw std::invalid_argument("metadata records must be all, files, or folders");
    }

    NfsMetaReader meta_reader(meta_config);
    NfsDataReader data_reader(data_config);

    HashInventoryReport report;
    report.meta_reader_async = meta_reader.using_async_backend();
    report.data_reader_async = data_reader.using_async_backend();
    report.meta_reader_threads = std::max<std::size_t>(1, meta_config.worker_count);
    report.metadata_async_depth = std::max<std::size_t>(1, meta_config.async_directory_depth);
    report.data_reader_threads = std::max<std::size_t>(1, data_config.data_reader_worker_count);
    report.data_outstanding_requests = std::max<std::size_t>(1, data_config.outstanding_requests);
    report.hash_worker_threads = hash_worker_threads == 0
                                     ? std::max<std::size_t>(1, hasher_config.worker_count)
                                     : std::max<std::size_t>(1, hash_worker_threads);
    report.max_files_queued = std::max<std::size_t>(1, max_files_queued);
    report.max_hash_chunks_queued = std::max<std::size_t>(1, max_hash_chunks_queued);
    report.hash_block_size = std::max<std::uint64_t>(1, hash_block_size);
    report.hash_mode = to_string(hash_mode);
    report.hash_algorithm = to_string(algorithm);

    MetadataRecordWriter record_writer(writer_config);
    const HashInventorySnapshot snapshot =
        run_parallel_hash_inventory_scan(meta_config,
                                         data_config,
                                         algorithm,
                                         hash_mode,
                                         report.hash_worker_threads,
                                         report.max_files_queued,
                                         report.max_hash_chunks_queued,
                                         report.hash_block_size,
                                         &record_writer,
                                         max_duration_seconds);
    record_writer.close();

    report.files_found = snapshot.files_found;
    report.folders_found = snapshot.folders_found;
    report.files_hashed = snapshot.files_hashed;
    report.files_failed = snapshot.files_failed;
    report.logical_size_bytes = snapshot.logical_size_bytes;
    report.bytes_read = snapshot.bytes_read;
    report.bytes_hashed = snapshot.bytes_hashed;
    report.read_bytes_per_second = snapshot.read_bytes_per_second;
    report.read_gigabits_per_second = snapshot.read_gigabits_per_second;
    report.bytes_per_second = snapshot.bytes_per_second;
    report.read_elapsed_seconds = snapshot.read_elapsed_seconds;
    report.elapsed_seconds = snapshot.elapsed_seconds;
    report.metadata_files_written = record_writer.files_written();
    report.metadata_folders_written = record_writer.folders_written();
    return report;
}

HashSpeedBenchmarkReport TransferEngine::benchmark_hash_speed(const std::string& hash_algorithm,
                                                              std::size_t worker_threads,
                                                              std::uint64_t block_size,
                                                              double duration_seconds,
                                                              double min_gigabits_per_core_second) const {
    const ContentHashAlgorithm algorithm = parse_content_hash_algorithm(hash_algorithm);
    const std::size_t threads = std::max<std::size_t>(1, worker_threads);
    const std::size_t payload_size = static_cast<std::size_t>(std::max<std::uint64_t>(1, block_size));
    const std::string payload = make_hash_benchmark_payload(payload_size);

    std::atomic<std::size_t> ready {0};
    std::atomic<bool> start {false};
    std::atomic<bool> stop {false};
    std::vector<HashSpeedWorkerResult> results(threads);
    std::vector<std::thread> workers;
    workers.reserve(threads);

    for (std::size_t index = 0; index < threads; ++index) {
        workers.emplace_back([&, index] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            HashSpeedWorkerResult local;
            std::uint64_t mix = 0x9e3779b97f4a7c15ULL ^ static_cast<std::uint64_t>(index);
            while (!stop.load(std::memory_order_relaxed)) {
                const std::uint64_t digest = benchmark_hash_once(algorithm, payload);
                mix ^= digest + 0x9e3779b97f4a7c15ULL + (mix << 6U) + (mix >> 2U);
                local.bytes_hashed += payload.size();
                ++local.iterations;
            }
            local.digest_mix = mix;
            results[index] = local;
        });
    }

    while (ready.load(std::memory_order_acquire) != threads) {
        std::this_thread::yield();
    }
    const auto started_at = std::chrono::steady_clock::now();
    start.store(true, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::duration<double>(duration_seconds));
    stop.store(true, std::memory_order_release);
    for (auto& worker : workers) {
        worker.join();
    }
    const auto ended_at = std::chrono::steady_clock::now();

    HashSpeedBenchmarkReport report;
    report.worker_threads = threads;
    report.hardware_threads = std::max<unsigned>(1U, std::thread::hardware_concurrency());
    report.block_size = payload.size();
    report.duration_seconds = duration_seconds;
    report.elapsed_seconds = std::chrono::duration<double>(ended_at - started_at).count();
    report.hash_algorithm = to_string(algorithm);
    report.min_gigabits_per_core_second = std::max(0.0, min_gigabits_per_core_second);
    for (const auto& result : results) {
        report.bytes_hashed += result.bytes_hashed;
        report.iterations += result.iterations;
        report.digest_mix ^= result.digest_mix;
    }
    report.bytes_per_second =
        report.elapsed_seconds > 0.0 ? static_cast<double>(report.bytes_hashed) / report.elapsed_seconds : 0.0;
    report.gigabits_per_second = report.bytes_per_second * 8.0 / 1'000'000'000.0;
    report.bytes_per_core_second =
        threads != 0 ? report.bytes_per_second / static_cast<double>(threads) : 0.0;
    report.gigabits_per_core_second = report.gigabits_per_second / static_cast<double>(threads);
    report.gigabits_per_hardware_core_second =
        report.gigabits_per_second / static_cast<double>(report.hardware_threads);
    report.passed = report.min_gigabits_per_core_second <= 0.0 ||
                    report.gigabits_per_core_second >= report.min_gigabits_per_core_second;
    return report;
}

MetadataWriterBenchmarkReport TransferEngine::benchmark_metadata_writer(
    const std::filesystem::path& output_path,
    const std::string& output_format,
    std::uint64_t file_count,
    std::uint64_t folder_count,
    std::size_t batch_size,
    std::uint64_t average_file_size,
    const std::string& duckdb_memory_limit,
    std::size_t duckdb_threads,
    const std::string& duckdb_checkpoint_threshold,
    const std::string& parquet_compression,
    std::size_t partitions,
    const std::string& partition_mode) const {
    if (output_path.empty()) {
        throw std::invalid_argument("metadata writer benchmark output path is required");
    }
    if (partition_mode != "threads" &&
        partition_mode != "processes" &&
        partition_mode != "transport-processes" &&
        partition_mode != "generate-discard" &&
        partition_mode != "generate-hash-discard" &&
        partition_mode != "pack-discard" &&
        partition_mode != "folder-pack-discard" &&
        partition_mode != "transport-discard") {
        throw std::invalid_argument("metadata writer benchmark partition mode must be threads, processes, transport-processes, generate-discard, generate-hash-discard, pack-discard, folder-pack-discard, or transport-discard");
    }
    partitions = std::max<std::size_t>(1U, partitions);

    MetadataRecordWriterConfig writer_config = load_metadata_record_writer_config(config_store_);
    writer_config.enabled = true;
    writer_config.output_path = output_path;
    writer_config.format = output_format.empty() || output_format == "auto"
                               ? infer_metadata_record_format(output_path)
                               : parse_metadata_record_format(output_format);
    writer_config.write_files = true;
    writer_config.write_folders = true;
    writer_config.flush_per_batch = false;
    if (!duckdb_memory_limit.empty()) {
        writer_config.duckdb_memory_limit = duckdb_memory_limit;
    }
    if (duckdb_threads != 0U) {
        writer_config.duckdb_threads = duckdb_threads;
    }
    if (!duckdb_checkpoint_threshold.empty()) {
        writer_config.duckdb_checkpoint_threshold = duckdb_checkpoint_threshold;
    }
    if (!parquet_compression.empty()) {
        writer_config.parquet_compression = parquet_compression;
    }

    if (partitions > 1U) {
        std::filesystem::create_directories(output_path);
    }

    const std::size_t effective_batch_size = std::max<std::size_t>(1U, batch_size);
    const auto count_for_partition = [partitions](std::uint64_t total, std::size_t index) {
        const std::uint64_t base = total / partitions;
        const std::uint64_t remainder = total % partitions;
        return base + (index < remainder ? 1U : 0U);
    };
    const auto part_suffix = [](std::size_t index) {
        std::ostringstream out;
        out << std::setw(5) << std::setfill('0') << index;
        return out.str();
    };
    const auto extension_for_format = [](MetadataRecordFormat format) {
        switch (format) {
            case MetadataRecordFormat::text:
                return std::string("txt");
            case MetadataRecordFormat::csv:
                return std::string("csv");
            case MetadataRecordFormat::parquet:
                return std::string("parquet");
        }
        return std::string("txt");
    };
    const auto output_for_partition = [&](std::size_t index) {
        if (partitions == 1U) {
            return output_path;
        }
        return output_path / ("part-" + part_suffix(index) + "." + extension_for_format(writer_config.format));
    };
    const auto report_for_partition = [&](std::size_t index) {
        return output_path / ("part-" + part_suffix(index) + ".report");
    };
    const auto socket_for_partition = [&](std::size_t index) {
        return output_path / ("part-" + part_suffix(index) + ".sock");
    };
    const auto metadata_partition_for_path = [partitions](std::string_view path) {
        return static_cast<std::size_t>(hash64(path) % partitions);
    };

    const auto run_partition = [&](std::size_t partition_index) {
        MetadataRecordWriterConfig partition_writer_config = writer_config;
        partition_writer_config.output_path = output_for_partition(partition_index);

        MetadataScanRunInfo run_info;
        run_info.run_id = partitions == 1U ? "metadata-writer-benchmark"
                                           : "metadata-writer-benchmark-part-" + part_suffix(partition_index);
        run_info.started_at_utc = "benchmark";
        run_info.source_root = "synthetic";
        run_info.settings_json = "{\"mode\":\"metadata_writer_benchmark\",\"partitions\":" +
                                 std::to_string(partitions) + ",\"partition\":" +
                                 std::to_string(partition_index) + "}";
        partition_writer_config.run_info = run_info;

        FileMetadataGenerator generator(FileMetadataGeneratorConfig(
            count_for_partition(file_count, partition_index),
            count_for_partition(folder_count, partition_index),
            effective_batch_size,
            average_file_size,
            0x9e3779b97f4a7c15ULL ^ (static_cast<std::uint64_t>(partition_index) * 0x94d049bb133111ebULL),
            partitions == 1U ? "generated" : "generated/part_" + part_suffix(partition_index)));
        MetadataRecordWriter writer(partition_writer_config);
        FileMetadataGeneratorBatch batch;

        MetadataWriterBenchmarkReport partition_report;
        while (generator.next_batch(batch)) {
            for (const auto& file : batch.files) {
                partition_report.logical_size_bytes += file.declared_size;
            }
            writer.write_batch(batch.files, batch.folders);
        }
        writer.close();

        partition_report.files_generated = generator.files_generated();
        partition_report.folders_generated = generator.folders_generated();
        partition_report.records_written = writer.files_written() + writer.folders_written();
        partition_report.batch_size = effective_batch_size;
        partition_report.partitions = partitions;
        partition_report.partition_mode = partition_mode;
        partition_report.output_format = to_string(writer_config.format);
        partition_report.output_path = partition_writer_config.output_path;
        return partition_report;
    };
    const auto write_partition_report = [](const std::filesystem::path& path,
                                           const MetadataWriterBenchmarkReport& report) {
        std::ofstream output(path, std::ios::out | std::ios::trunc);
        if (!output) {
            throw std::runtime_error("failed to open metadata writer partition report: " + path.string());
        }
        output << report.files_generated << ' '
               << report.folders_generated << ' '
               << report.records_written << ' '
               << report.logical_size_bytes << '\n';
    };
    const auto read_partition_report = [&](std::size_t index) {
        const std::filesystem::path path = report_for_partition(index);
        std::ifstream input(path);
        if (!input) {
            throw std::runtime_error("failed to read metadata writer partition report: " + path.string());
        }
        MetadataWriterBenchmarkReport report;
        input >> report.files_generated
              >> report.folders_generated
              >> report.records_written
              >> report.logical_size_bytes;
        if (!input) {
            throw std::runtime_error("malformed metadata writer partition report: " + path.string());
        }
        report.batch_size = effective_batch_size;
        report.partitions = partitions;
        report.partition_mode = partition_mode;
        report.output_format = to_string(writer_config.format);
        report.output_path = output_for_partition(index);
        std::filesystem::remove(path);
        return report;
    };
    const auto run_transport_writer_process = [&](std::size_t partition_index) {
        MetadataRecordWriterConfig partition_writer_config = writer_config;
        partition_writer_config.output_path = output_for_partition(partition_index);

        MetadataScanRunInfo run_info;
        run_info.run_id = "metadata-writer-transport-part-" + part_suffix(partition_index);
        run_info.started_at_utc = "benchmark";
        run_info.source_root = "synthetic";
        run_info.settings_json = "{\"mode\":\"metadata_writer_transport_benchmark\",\"partitions\":" +
                                 std::to_string(partitions) + ",\"partition\":" +
                                 std::to_string(partition_index) + "}";
        partition_writer_config.run_info = run_info;

        RawBufferPool receive_pool(kMetadataBatchBufferPoolId,
                                   16U,
                                   sizeof(MetadataBatchBuffer),
                                   alignof(MetadataBatchBuffer));
        BufferPoolRegistry receive_registry;
        receive_registry.register_pool(receive_pool);
        BufQueue writer_queue(receive_pool.capacity());
        BufferReceiverJob receiver(1U,
                                   receive_pool,
                                   writer_queue,
                                   BufferTransportEndpoint::unix_socket(socket_for_partition(partition_index)));
        MetadataRecordWriterJob writer(1U, writer_queue, receive_registry, partition_writer_config);

        receiver.start();
        writer.start();
        receiver.wait();
        writer.wait();

        MetadataWriterBenchmarkReport partition_report;
        const auto stats = writer.writer_stats();
        partition_report.files_generated = stats.files_written;
        partition_report.folders_generated = stats.folders_written;
        partition_report.records_written = stats.files_written + stats.folders_written;
        partition_report.batch_size = effective_batch_size;
        partition_report.partitions = partitions;
        partition_report.partition_mode = partition_mode;
        partition_report.output_format = to_string(writer_config.format);
        partition_report.output_path = partition_writer_config.output_path;
        return partition_report;
    };
    const auto run_transport_discard_process = [&](std::size_t partition_index) {
        RawBufferPool receive_pool(kMetadataBatchBufferPoolId,
                                   16U,
                                   sizeof(MetadataBatchBuffer),
                                   alignof(MetadataBatchBuffer));
        BufferPoolRegistry receive_registry;
        receive_registry.register_pool(receive_pool);
        BufQueue discard_queue(receive_pool.capacity());
        BufferReceiverJob receiver(1U,
                                   receive_pool,
                                   discard_queue,
                                   BufferTransportEndpoint::unix_socket(socket_for_partition(partition_index)));
        BufferDiscarderJob discarder(BufferDiscarderConfig(1U), discard_queue, receive_registry);

        receiver.start();
        discarder.start();
        receiver.wait();
        discarder.wait();

        MetadataWriterBenchmarkReport partition_report;
        const auto stats = discarder.stats();
        partition_report.records_written = stats.buffers_discarded;
        partition_report.logical_size_bytes = stats.bytes_discarded;
        partition_report.batch_size = effective_batch_size;
        partition_report.partitions = partitions;
        partition_report.partition_mode = partition_mode;
        partition_report.output_format = "discard";
        partition_report.output_path = output_for_partition(partition_index);
        return partition_report;
    };

    std::vector<MetadataWriterBenchmarkReport> partition_reports(partitions);
    std::exception_ptr worker_error;
    std::mutex worker_error_mutex;
    std::uint64_t transport_logical_size_bytes = 0;
    std::uint64_t transport_files_generated = 0;
    std::uint64_t transport_folders_generated = 0;

    const auto run_parent_generator_probe = [&](bool hash_paths) {
        FileMetadataGenerator generator(FileMetadataGeneratorConfig(file_count,
                                                                    folder_count,
                                                                    effective_batch_size,
                                                                    average_file_size,
                                                                    0x9e3779b97f4a7c15ULL,
                                                                    "generated"));
        std::uint64_t shard_mix = 0;
        for (std::uint64_t first = 0; first < file_count; first += effective_batch_size) {
            const std::uint64_t count = std::min<std::uint64_t>(effective_batch_size, file_count - first);
            generator.for_each_file_view(first, count, [&](const GeneratedFileMetadataView& file) {
                transport_logical_size_bytes += file.declared_size;
                ++transport_files_generated;
                if (hash_paths) {
                    shard_mix += metadata_partition_for_path(file.rel_path);
                }
            });
        }
        for (std::uint64_t first = 0; first < folder_count; first += effective_batch_size) {
            const std::uint64_t count = std::min<std::uint64_t>(effective_batch_size, folder_count - first);
            generator.for_each_folder_view(first, count, [&](const GeneratedFolderMetadataView& folder) {
                ++transport_folders_generated;
                if (hash_paths) {
                    shard_mix += metadata_partition_for_path(folder.rel_path);
                }
            });
        }
        if (hash_paths && shard_mix == UINT64_MAX) {
            throw std::runtime_error("unexpected metadata shard checksum");
        }
    };

    const auto run_parent_batch_producer = [&](bool send_to_children) {
        RawBufferPool send_pool(kMetadataBatchBufferPoolId,
                                std::max<std::size_t>(64U, partitions * 4U),
                                sizeof(MetadataBatchBuffer),
                                alignof(MetadataBatchBuffer));
        BufferPoolRegistry send_registry;
        send_registry.register_pool(send_pool);
        std::vector<std::unique_ptr<BufQueue>> sender_queues;
        std::vector<std::unique_ptr<BufferSenderJob>> senders;
        if (send_to_children) {
            sender_queues.reserve(partitions);
            senders.reserve(partitions);
            for (std::size_t index = 0; index < partitions; ++index) {
                sender_queues.push_back(std::make_unique<BufQueue>(std::max<std::size_t>(4U, send_pool.capacity() / partitions)));
                senders.push_back(std::make_unique<BufferSenderJob>(
                    1U,
                    *sender_queues.back(),
                    send_registry,
                    BufferTransportEndpoint::unix_socket(socket_for_partition(index)),
                    metadata_transport_payload_bytes));
                senders.back()->start();
            }
        }

        std::vector<BufferHandle> open_batches(partitions);
        std::vector<bool> has_open_batch(partitions, false);
        const auto acquire_batch = [&](std::size_t partition_index) -> MetadataBatchBuffer& {
            if (!has_open_batch[partition_index]) {
                open_batches[partition_index] = send_pool.acquire_spin();
                reset_metadata_batch(metadata_batch_buffer(send_pool, open_batches[partition_index]));
                has_open_batch[partition_index] = true;
            }
            return metadata_batch_buffer(send_pool, open_batches[partition_index]);
        };
        const auto flush_batch = [&](std::size_t partition_index) {
            if (!has_open_batch[partition_index]) {
                return;
            }
            BufferHandle handle = open_batches[partition_index];
            if (metadata_batch_buffer(send_pool, handle).record_count == 0U) {
                send_pool.release(handle);
            } else if (send_to_children) {
                if (!sender_queues[partition_index]->push_wait(handle)) {
                    send_pool.release(handle);
                    throw std::runtime_error("metadata sender queue closed while pushing batch");
                }
            } else {
                send_pool.release(handle);
            }
            has_open_batch[partition_index] = false;
        };

        FileMetadataGenerator generator(FileMetadataGeneratorConfig(file_count,
                                                                    folder_count,
                                                                    effective_batch_size,
                                                                    average_file_size,
                                                                    0x9e3779b97f4a7c15ULL,
                                                                    "generated"));
        std::uint64_t next_folder = 0;
        const std::uint64_t folder_budget = std::max<std::size_t>(1U, effective_batch_size / 1024U);
        const auto emit_folders = [&](std::uint64_t requested) {
            if (next_folder >= folder_count) {
                return;
            }
            const std::uint64_t count = std::min<std::uint64_t>(requested, folder_count - next_folder);
            generator.for_each_folder_view(next_folder, count, [&](const GeneratedFolderMetadataView& folder) {
                ++transport_folders_generated;
                const std::size_t partition_index = metadata_partition_for_path(folder.rel_path);
                if (!append_metadata_batch_folder(acquire_batch(partition_index), folder)) {
                    flush_batch(partition_index);
                    if (!append_metadata_batch_folder(acquire_batch(partition_index), folder)) {
                        throw std::runtime_error("metadata folder record does not fit metadata batch buffer");
                    }
                }
            });
            next_folder += count;
        };
        for (std::uint64_t first = 0; first < file_count; first += effective_batch_size) {
            const std::uint64_t count = std::min<std::uint64_t>(effective_batch_size, file_count - first);
            generator.for_each_file_view(first, count, [&](const GeneratedFileMetadataView& file) {
                transport_logical_size_bytes += file.declared_size;
                ++transport_files_generated;
                const std::size_t partition_index = metadata_partition_for_path(file.rel_path);
                if (!append_metadata_batch_file(acquire_batch(partition_index), file)) {
                    flush_batch(partition_index);
                    if (!append_metadata_batch_file(acquire_batch(partition_index), file)) {
                        throw std::runtime_error("metadata file record does not fit metadata batch buffer");
                    }
                }
            });
            emit_folders(folder_budget);
        }
        while (next_folder < folder_count) {
            emit_folders(folder_budget);
        }
        for (std::size_t index = 0; index < partitions; ++index) {
            flush_batch(index);
        }
        if (send_to_children) {
            for (auto& queue : sender_queues) {
                queue->close();
            }
            for (auto& sender : senders) {
                sender->wait();
            }
        }
    };

    const auto run_parent_folder_batch_producer = [&]() {
        RawBufferPool send_pool(kMetadataBatchBufferPoolId,
                                std::max<std::size_t>(64U, partitions * 4U),
                                sizeof(MetadataBatchBuffer),
                                alignof(MetadataBatchBuffer));
        const auto make_folder_record = [](const GeneratedFolderMetadataView& view) {
            MetadataFolderRecord folder;
            folder.spec.rel_path.assign(view.rel_path);
            folder.spec.mtime = view.mtime;
            folder.spec.mode = view.mode;
            folder.spec.uid = view.uid;
            folder.spec.gid = view.gid;
            folder.flat_file_count = static_cast<std::size_t>(view.flat_file_count);
            folder.flat_logical_size_bytes = view.flat_logical_size_bytes;
            return folder;
        };

        FileMetadataGenerator generator(FileMetadataGeneratorConfig(file_count,
                                                                    folder_count,
                                                                    effective_batch_size,
                                                                    average_file_size,
                                                                    0x9e3779b97f4a7c15ULL,
                                                                    "generated"));
        const std::uint64_t effective_folder_count = std::max<std::uint64_t>(1U, folder_count);
        for (std::uint64_t folder_index = 0; folder_index < effective_folder_count; ++folder_index) {
            MetadataFolderRecord folder_record;
            generator.for_each_folder_view(folder_index, 1U, [&](const GeneratedFolderMetadataView& folder) {
                folder_record = make_folder_record(folder);
            });
            const bool include_folder_record = folder_index < folder_count;
            if (include_folder_record) {
                ++transport_folders_generated;
            }

            BufferHandle batch_handle = send_pool.acquire_spin();
            if (!reset_folder_metadata_batch(metadata_batch_buffer(send_pool, batch_handle),
                                             folder_record,
                                             include_folder_record)) {
                send_pool.release(batch_handle);
                throw std::runtime_error("metadata folder path does not fit metadata batch buffer");
            }

            for (std::uint64_t file_index = folder_index;
                 file_index < file_count;
                 file_index += effective_folder_count) {
                bool appended = false;
                generator.for_each_file_view(file_index, 1U, [&](const GeneratedFileMetadataView& file) {
                    transport_logical_size_bytes += file.declared_size;
                    ++transport_files_generated;
                    appended = append_folder_metadata_batch_file(metadata_batch_buffer(send_pool, batch_handle),
                                                                 file);
                });
                if (!appended) {
                    send_pool.release(batch_handle);
                    batch_handle = send_pool.acquire_spin();
                    if (!reset_folder_metadata_batch(metadata_batch_buffer(send_pool, batch_handle),
                                                     folder_record,
                                                     false)) {
                        send_pool.release(batch_handle);
                        throw std::runtime_error("metadata folder path does not fit metadata batch buffer");
                    }
                    generator.for_each_file_view(file_index, 1U, [&](const GeneratedFileMetadataView& file) {
                        if (!append_folder_metadata_batch_file(metadata_batch_buffer(send_pool, batch_handle),
                                                               file)) {
                            throw std::runtime_error("metadata file record does not fit folder metadata batch buffer");
                        }
                    });
                }
            }
            send_pool.release(batch_handle);
        }
    };

    const auto started_at = std::chrono::steady_clock::now();
    if (partition_mode == "generate-discard") {
        run_parent_generator_probe(false);
    } else if (partition_mode == "generate-hash-discard") {
        run_parent_generator_probe(true);
    } else if (partition_mode == "pack-discard") {
        run_parent_batch_producer(false);
    } else if (partition_mode == "folder-pack-discard") {
        run_parent_folder_batch_producer();
    } else if (partitions == 1U) {
        partition_reports.front() = run_partition(0);
    } else if (partition_mode == "processes" || partition_mode == "transport-processes" || partition_mode == "transport-discard") {
        std::vector<pid_t> children;
        children.reserve(partitions);
        for (std::size_t index = 0; index < partitions; ++index) {
            std::filesystem::remove(report_for_partition(index));
            std::filesystem::remove(socket_for_partition(index));
            const pid_t child = ::fork();
            if (child < 0) {
                throw std::runtime_error("failed to fork metadata writer partition process");
            }
            if (child == 0) {
                try {
                    write_partition_report(report_for_partition(index),
                                           partition_mode == "transport-processes"
                                               ? run_transport_writer_process(index)
                                               : partition_mode == "transport-discard"
                                                     ? run_transport_discard_process(index)
                                                     : run_partition(index));
                    _Exit(0);
                } catch (const std::exception& error) {
                    std::cerr << "metadata writer partition " << index << " failed: "
                              << error.what() << '\n';
                    _Exit(101);
                } catch (...) {
                    std::cerr << "metadata writer partition " << index << " failed\n";
                    _Exit(101);
                }
            }
            children.push_back(child);
        }

        if (partition_mode == "transport-processes" || partition_mode == "transport-discard") {
            run_parent_batch_producer(true);
        }

        bool child_failed = false;
        for (const pid_t child : children) {
            int status = 0;
            if (::waitpid(child, &status, 0) < 0) {
                throw std::runtime_error("failed waiting for metadata writer partition process");
            }
            if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
                child_failed = true;
            }
        }
        if (child_failed) {
            throw std::runtime_error("one or more metadata writer partition processes failed");
        }
        for (std::size_t index = 0; index < partitions; ++index) {
            partition_reports[index] = read_partition_report(index);
            std::filesystem::remove(socket_for_partition(index));
        }
    } else {
        std::vector<std::thread> workers;
        workers.reserve(partitions);
        for (std::size_t index = 0; index < partitions; ++index) {
            workers.emplace_back([&, index]() {
                try {
                    partition_reports[index] = run_partition(index);
                } catch (...) {
                    std::lock_guard<std::mutex> lock(worker_error_mutex);
                    if (!worker_error) {
                        worker_error = std::current_exception();
                    }
                }
            });
        }
        for (auto& worker : workers) {
            worker.join();
        }
        if (worker_error) {
            std::rethrow_exception(worker_error);
        }
    }
    const auto ended_at = std::chrono::steady_clock::now();

    MetadataWriterBenchmarkReport report;
    for (const auto& partition_report : partition_reports) {
        report.files_generated += partition_report.files_generated;
        report.folders_generated += partition_report.folders_generated;
        report.records_written += partition_report.records_written;
        report.logical_size_bytes += partition_report.logical_size_bytes;
    }
    if (partition_mode == "generate-discard" ||
        partition_mode == "generate-hash-discard" ||
        partition_mode == "pack-discard" ||
        partition_mode == "folder-pack-discard" ||
        partition_mode == "transport-processes" ||
        partition_mode == "transport-discard") {
        report.files_generated = transport_files_generated;
        report.folders_generated = transport_folders_generated;
        report.records_written = transport_files_generated + transport_folders_generated;
        report.logical_size_bytes = transport_logical_size_bytes;
    }
    report.elapsed_seconds = std::chrono::duration<double>(ended_at - started_at).count();
    report.records_per_second = report.elapsed_seconds > 0.0
                                    ? static_cast<double>(report.records_written) / report.elapsed_seconds
                                    : 0.0;
    report.files_per_second = report.elapsed_seconds > 0.0
                                  ? static_cast<double>(report.files_generated) / report.elapsed_seconds
                                  : 0.0;
    report.batch_size = effective_batch_size;
    report.partitions = partitions;
    report.partition_mode = partition_mode;
    report.output_format = to_string(writer_config.format);
    report.output_path = output_path;
    return report;
}

namespace {

constexpr std::uint64_t kSharedNothingBufferFrameMagic = 0x5753594e43425546ULL;  // WSYNCBUF
constexpr std::uint32_t kSharedNothingBufferFrameVersion = 1;

void shared_nothing_write_u16_be(std::array<std::byte, 32>& out,
                                 std::size_t offset,
                                 std::uint16_t value) noexcept {
    out[offset] = static_cast<std::byte>((value >> 8U) & 0xFFU);
    out[offset + 1U] = static_cast<std::byte>(value & 0xFFU);
}

void shared_nothing_write_u32_be(std::array<std::byte, 32>& out,
                                 std::size_t offset,
                                 std::uint32_t value) noexcept {
    for (int shift = 24; shift >= 0; shift -= 8) {
        out[offset++] = static_cast<std::byte>((value >> static_cast<unsigned>(shift)) & 0xFFU);
    }
}

void shared_nothing_write_u64_be(std::array<std::byte, 32>& out,
                                 std::size_t offset,
                                 std::uint64_t value) noexcept {
    for (int shift = 56; shift >= 0; shift -= 8) {
        out[offset++] = static_cast<std::byte>((value >> static_cast<unsigned>(shift)) & 0xFFU);
    }
}

std::uint32_t shared_nothing_read_u32_be(const std::array<std::byte, 32>& in,
                                         std::size_t offset) noexcept {
    const auto* bytes = reinterpret_cast<const unsigned char*>(in.data() + offset);
    return (static_cast<std::uint32_t>(bytes[0]) << 24U) |
           (static_cast<std::uint32_t>(bytes[1]) << 16U) |
           (static_cast<std::uint32_t>(bytes[2]) << 8U) |
           static_cast<std::uint32_t>(bytes[3]);
}

std::uint64_t shared_nothing_read_u64_be(const std::array<std::byte, 32>& in,
                                         std::size_t offset) noexcept {
    const auto* bytes = reinterpret_cast<const unsigned char*>(in.data() + offset);
    std::uint64_t value = 0;
    for (int index = 0; index < 8; ++index) {
        value = (value << 8U) | static_cast<std::uint64_t>(bytes[index]);
    }
    return value;
}

std::array<std::byte, 32> make_shared_nothing_header(std::size_t payload_bytes) {
    if (payload_bytes > std::numeric_limits<std::uint32_t>::max()) {
        throw std::overflow_error("shared-nothing transport payload too large");
    }
    std::array<std::byte, 32> header {};
    shared_nothing_write_u64_be(header, 0, kSharedNothingBufferFrameMagic);
    shared_nothing_write_u32_be(header, 8, kSharedNothingBufferFrameVersion);
    shared_nothing_write_u16_be(header, 12, kDataBufferPoolId);
    shared_nothing_write_u32_be(header, 16, static_cast<std::uint32_t>(payload_bytes));
    return header;
}

std::uint32_t parse_shared_nothing_header(const std::array<std::byte, 32>& header) {
    if (shared_nothing_read_u64_be(header, 0) != kSharedNothingBufferFrameMagic) {
        throw std::runtime_error("invalid shared-nothing buffer frame magic");
    }
    if (shared_nothing_read_u32_be(header, 8) != kSharedNothingBufferFrameVersion) {
        throw std::runtime_error("unsupported shared-nothing buffer frame version");
    }
    return shared_nothing_read_u32_be(header, 16);
}

void write_shared_nothing_frame(int fd,
                                const std::array<std::byte, 32>& header,
                                const std::byte* payload,
                                std::size_t payload_bytes) {
    std::array<iovec, 2> iov {{
        {const_cast<std::byte*>(header.data()), header.size()},
        {const_cast<std::byte*>(payload), payload_bytes},
    }};
    int iov_count = payload_bytes == 0U ? 1 : 2;
    while (iov_count > 0) {
        const ssize_t written = ::writev(fd, iov.data(), iov_count);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw std::system_error(errno, std::generic_category(), "shared-nothing writev failed");
        }
        if (written == 0) {
            throw std::runtime_error("shared-nothing writev wrote zero bytes");
        }
        std::size_t remaining = static_cast<std::size_t>(written);
        while (iov_count > 0 && remaining >= iov.front().iov_len) {
            remaining -= iov.front().iov_len;
            iov[0] = iov[1];
            --iov_count;
        }
        if (iov_count > 0 && remaining != 0U) {
            iov.front().iov_base = static_cast<std::byte*>(iov.front().iov_base) + remaining;
            iov.front().iov_len -= remaining;
        }
    }
}

void fill_shared_nothing_payload(std::vector<std::byte>& payload,
                                 BufferGeneratorPattern pattern,
                                 std::uint64_t seed) {
    if (pattern == BufferGeneratorPattern::zero) {
        std::memset(payload.data(), 0, payload.size());
        return;
    }
    std::uint64_t state = seed == 0U ? 0x9e3779b97f4a7c15ULL : seed;
    for (std::size_t offset = 0; offset < payload.size(); ++offset) {
        state ^= state >> 12U;
        state ^= state << 25U;
        state ^= state >> 27U;
        const std::uint64_t value = state * 0x2545F4914F6CDD1DULL;
        const unsigned char byte =
            pattern == BufferGeneratorPattern::fast_text
                ? static_cast<unsigned char>('A' + (value % 26U))
                : static_cast<unsigned char>(value & 0xFFU);
        payload[offset] = static_cast<std::byte>(byte);
    }
}

ScopedFd accept_shared_nothing_tcp(int listen_fd) {
    for (;;) {
        try {
            return accept_tcp(listen_fd);
        } catch (const std::system_error& error) {
            if (error.code().value() == EINTR || error.code().value() == ECONNABORTED) {
                continue;
            }
            throw;
        }
    }
}

}  // namespace

BufferTransportBenchmarkReport TransferEngine::benchmark_buffer_transport(
    std::size_t transports,
    std::uint64_t buffers_per_transport,
    std::size_t buffer_size,
    std::size_t pool_slots_per_transport,
    std::size_t generator_threads,
    std::size_t sender_threads,
    std::size_t receiver_threads,
    std::size_t discarder_threads,
    const std::string& pattern,
    const std::string& transport_kind,
    std::uint16_t base_port,
    const std::filesystem::path& socket_dir,
    bool shared_input_queue,
    bool shared_nothing,
    const std::string& remote_role,
    const std::string& tcp_host) const {
    if (transports == 0U ||
        buffers_per_transport == 0U ||
        buffer_size == 0U ||
        pool_slots_per_transport == 0U ||
        generator_threads == 0U ||
        sender_threads == 0U ||
        receiver_threads == 0U ||
        discarder_threads == 0U) {
        throw std::invalid_argument("buffer transport benchmark numeric parameters must be positive");
    }
    if (transport_kind != "none" && transport_kind != "unix" && transport_kind != "tcp") {
        throw std::invalid_argument("buffer transport benchmark transport must be none, unix, or tcp");
    }
    if (remote_role != "local" && remote_role != "sender" && remote_role != "receiver") {
        throw std::invalid_argument("buffer transport benchmark role must be local, sender, or receiver");
    }
    if (remote_role != "local" && transport_kind != "tcp") {
        throw std::invalid_argument("remote buffer transport benchmark roles require --transport tcp");
    }
    if (transport_kind == "tcp" &&
        static_cast<unsigned>(base_port) + transports - 1U > 65535U) {
        throw std::invalid_argument("buffer transport benchmark tcp port range exceeds 65535");
    }

    const BufferGeneratorPattern generator_pattern = parse_buffer_generator_pattern(pattern);
    const std::filesystem::path effective_socket_dir =
        socket_dir.empty()
            ? std::filesystem::temp_directory_path() /
                  ("wsync-transport-bench-" + std::to_string(static_cast<unsigned long long>(::getpid())))
            : socket_dir;
    if (transport_kind == "unix") {
        std::filesystem::create_directories(effective_socket_dir);
    }

    if (shared_nothing) {
        if (transport_kind != "tcp" || (remote_role != "sender" && remote_role != "receiver")) {
            throw std::invalid_argument("shared-nothing transport benchmark requires --transport tcp and --role sender|receiver");
        }

        std::atomic<std::uint64_t> buffers_processed {0};
        std::atomic<std::uint64_t> payload_bytes_processed {0};
        std::atomic<std::uint64_t> first_receive_ns {0};
        std::atomic<std::uint64_t> last_receive_ns {0};
        std::exception_ptr worker_error;
        std::mutex worker_error_mutex;
        auto capture_worker_error = [&]() {
            std::lock_guard<std::mutex> lock(worker_error_mutex);
            if (!worker_error) {
                worker_error = std::current_exception();
            }
        };

        const auto started_at = std::chrono::steady_clock::now();
        std::vector<std::thread> workers;
        workers.reserve(transports);
        std::vector<ScopedFd> listeners;

        if (remote_role == "receiver") {
            listeners.reserve(transports);
            for (std::size_t index = 0; index < transports; ++index) {
                listeners.push_back(listen_tcp(tcp_host,
                                               static_cast<std::uint16_t>(base_port + index),
                                               64));
            }
            for (std::size_t index = 0; index < transports; ++index) {
                workers.emplace_back([&, index]() {
                    try {
                        ScopedFd fd = accept_shared_nothing_tcp(listeners[index].get());
                        std::vector<std::byte> payload(buffer_size);
                        for (;;) {
                            std::array<std::byte, 32> header;
                            if (!read_exact_or_eof(fd.get(), header.data(), header.size())) {
                                break;
                            }
                            const std::uint32_t payload_bytes = parse_shared_nothing_header(header);
                            if (payload_bytes > payload.size()) {
                                throw std::runtime_error("shared-nothing frame exceeds receiver buffer size");
                            }
                            if (payload_bytes != 0U &&
                                !read_exact_or_eof(fd.get(), payload.data(), payload_bytes)) {
                                throw std::runtime_error("unexpected EOF while reading shared-nothing payload");
                            }
                            const auto frame_ns = static_cast<std::uint64_t>(
                                std::chrono::duration_cast<std::chrono::nanoseconds>(
                                    std::chrono::steady_clock::now().time_since_epoch())
                                    .count());
                            std::uint64_t expected = 0;
                            (void)first_receive_ns.compare_exchange_strong(expected,
                                                                            frame_ns,
                                                                            std::memory_order_relaxed,
                                                                            std::memory_order_relaxed);
                            last_receive_ns.store(frame_ns, std::memory_order_relaxed);
                            buffers_processed.fetch_add(1U, std::memory_order_relaxed);
                            payload_bytes_processed.fetch_add(payload_bytes, std::memory_order_relaxed);
                        }
                    } catch (...) {
                        capture_worker_error();
                    }
                });
            }
        } else {
            for (std::size_t index = 0; index < transports; ++index) {
                workers.emplace_back([&, index]() {
                    try {
                        ScopedFd fd = connect_tcp(tcp_host,
                                                  static_cast<std::uint16_t>(base_port + index),
                                                  500,
                                                  200);
                        std::vector<std::byte> payload(buffer_size);
                        fill_shared_nothing_payload(payload,
                                                    generator_pattern,
                                                    0x9e3779b97f4a7c15ULL ^ static_cast<std::uint64_t>(index));
                        const std::array<std::byte, 32> header =
                            make_shared_nothing_header(payload.size());
                        for (std::uint64_t sequence = 0; sequence < buffers_per_transport; ++sequence) {
                            write_shared_nothing_frame(fd.get(), header, payload.data(), payload.size());
                            buffers_processed.fetch_add(1U, std::memory_order_relaxed);
                            payload_bytes_processed.fetch_add(payload.size(), std::memory_order_relaxed);
                        }
                        ::shutdown(fd.get(), SHUT_WR);
                    } catch (...) {
                        capture_worker_error();
                    }
                });
            }
        }

        for (auto& worker : workers) {
            worker.join();
        }
        if (worker_error) {
            std::rethrow_exception(worker_error);
        }
        const auto ended_at = std::chrono::steady_clock::now();

        BufferTransportBenchmarkReport report;
        const std::uint64_t buffers = buffers_processed.load(std::memory_order_relaxed);
        const std::uint64_t payload_bytes = payload_bytes_processed.load(std::memory_order_relaxed);
        if (remote_role == "receiver") {
            report.buffers_received = buffers;
            report.buffers_discarded = buffers;
            report.payload_bytes_received = payload_bytes;
            report.receiver_threads = transports;
            report.discarder_threads = 0;
            report.transport_kind = "tcp-remote-receiver-shared-nothing";
        } else {
            report.buffers_generated = buffers;
            report.buffers_sent = buffers;
            report.payload_bytes_sent = payload_bytes;
            report.generator_threads = transports;
            report.sender_threads = transports;
            report.transport_kind = "tcp-remote-sender-shared-nothing";
        }
        report.elapsed_seconds = std::chrono::duration<double>(ended_at - started_at).count();
        if (remote_role == "receiver") {
            const std::uint64_t first_ns = first_receive_ns.load(std::memory_order_relaxed);
            const std::uint64_t last_ns = last_receive_ns.load(std::memory_order_relaxed);
            if (first_ns != 0U && last_ns > first_ns) {
                report.elapsed_seconds = static_cast<double>(last_ns - first_ns) / 1'000'000'000.0;
            }
        }
        report.bytes_per_second = report.elapsed_seconds > 0.0
                                      ? static_cast<double>(payload_bytes) / report.elapsed_seconds
                                      : 0.0;
        report.gigabytes_per_second = report.bytes_per_second / 1'000'000'000.0;
        report.gibibytes_per_second = report.bytes_per_second / (1024.0 * 1024.0 * 1024.0);
        report.gigabits_per_second = report.bytes_per_second * 8.0 / 1'000'000'000.0;
        report.transports = transports;
        report.buffer_size = buffer_size;
        report.pool_slots_per_transport = 1U;
        report.buffers_per_transport = buffers_per_transport;
        report.pattern = to_string(generator_pattern);
        return report;
    }

    if (remote_role == "receiver") {
        std::vector<std::unique_ptr<RawBufferPool>> receive_pools;
        std::vector<std::unique_ptr<BufferPoolRegistry>> receive_registries;
        std::vector<std::unique_ptr<BufQueue>> receiver_outputs;
        std::vector<std::unique_ptr<BufferReceiverJob>> receivers;
        std::vector<std::unique_ptr<BufferDiscarderJob>> discarders;
        receive_pools.reserve(transports);
        receive_registries.reserve(transports);
        receiver_outputs.reserve(transports);
        receivers.reserve(transports);
        discarders.reserve(transports);

        for (std::size_t index = 0; index < transports; ++index) {
            BufferTransportEndpoint endpoint =
                BufferTransportEndpoint::tcp(tcp_host, static_cast<std::uint16_t>(base_port + index));
            receive_pools.push_back(std::make_unique<RawBufferPool>(kDataBufferPoolId,
                                                                     pool_slots_per_transport,
                                                                     buffer_size));
            receive_registries.push_back(std::make_unique<BufferPoolRegistry>());
            receive_registries.back()->register_pool(*receive_pools.back());
            receiver_outputs.push_back(std::make_unique<BufQueue>(std::max<std::size_t>(1U, pool_slots_per_transport)));
            receivers.push_back(std::make_unique<BufferReceiverJob>(receiver_threads,
                                                                    *receive_pools.back(),
                                                                    *receiver_outputs.back(),
                                                                    endpoint));
            discarders.push_back(std::make_unique<BufferDiscarderJob>(BufferDiscarderConfig(discarder_threads),
                                                                      *receiver_outputs.back(),
                                                                      *receive_registries.back()));
        }

        const auto started_at = std::chrono::steady_clock::now();
        for (std::size_t index = 0; index < transports; ++index) {
            receivers[index]->start();
            discarders[index]->start();
        }
        for (auto& receiver : receivers) {
            receiver->wait();
        }
        for (auto& discarder : discarders) {
            discarder->wait();
        }
        const auto ended_at = std::chrono::steady_clock::now();

        BufferTransportBenchmarkReport report;
        for (std::size_t index = 0; index < transports; ++index) {
            const auto receiver_stats = receivers[index]->stats();
            const auto discarder_stats = discarders[index]->stats();
            report.buffers_received += receiver_stats.buffers;
            report.buffers_discarded += discarder_stats.buffers_discarded;
            report.payload_bytes_received += receiver_stats.payload_bytes;
        }
        report.elapsed_seconds = std::chrono::duration<double>(ended_at - started_at).count();
        report.bytes_per_second = report.elapsed_seconds > 0.0
                                      ? static_cast<double>(report.payload_bytes_received) / report.elapsed_seconds
                                      : 0.0;
        report.gigabytes_per_second = report.bytes_per_second / 1'000'000'000.0;
        report.gibibytes_per_second = report.bytes_per_second / (1024.0 * 1024.0 * 1024.0);
        report.gigabits_per_second = report.bytes_per_second * 8.0 / 1'000'000'000.0;
        report.transports = transports;
        report.generator_threads = 0;
        report.sender_threads = 0;
        report.receiver_threads = receiver_threads;
        report.discarder_threads = discarder_threads;
        report.buffer_size = buffer_size;
        report.pool_slots_per_transport = pool_slots_per_transport;
        report.buffers_per_transport = buffers_per_transport;
        report.pattern = to_string(generator_pattern);
        report.transport_kind = "tcp-remote-receiver";
        return report;
    }

    if (remote_role == "sender") {
        if (shared_input_queue) {
            const std::size_t total_pool_slots = pool_slots_per_transport * transports;
            const std::uint64_t total_buffers = buffers_per_transport * transports;
            RawBufferPool send_pool(kDataBufferPoolId, total_pool_slots, buffer_size);
            BufferPoolRegistry send_registry;
            send_registry.register_pool(send_pool);
            const std::size_t queue_depth_per_lane =
                std::max<std::size_t>(1U, pool_slots_per_transport);
            ShardedBufQueue sender_input(transports, queue_depth_per_lane);
            BufferGeneratorJob generator(BufferGeneratorConfig(generator_threads,
                                                               total_buffers,
                                                               generator_pattern,
                                                               0x9e3779b97f4a7c15ULL,
                                                               1.0),
                                         send_pool,
                                         sender_input);

            std::vector<std::unique_ptr<BufferSenderJob>> senders;
            senders.reserve(transports);
            for (std::size_t index = 0; index < transports; ++index) {
                senders.push_back(std::make_unique<BufferSenderJob>(
                    sender_threads,
                    sender_input.shard(index),
                    send_registry,
                    BufferTransportEndpoint::tcp(tcp_host, static_cast<std::uint16_t>(base_port + index))));
            }

            const auto started_at = std::chrono::steady_clock::now();
            for (auto& sender : senders) {
                sender->start();
            }
            generator.start();
            generator.wait();
            for (auto& sender : senders) {
                sender->wait();
            }
            const auto ended_at = std::chrono::steady_clock::now();

            BufferTransportBenchmarkReport report;
            const auto generator_stats = generator.stats();
            report.buffers_generated = generator_stats.buffers_generated;
            report.payload_bytes_sent = generator_stats.bytes_generated;
            for (const auto& sender : senders) {
                const auto sender_stats = sender->stats();
                report.buffers_sent += sender_stats.buffers;
            }
            report.elapsed_seconds = std::chrono::duration<double>(ended_at - started_at).count();
            report.bytes_per_second = report.elapsed_seconds > 0.0
                                          ? static_cast<double>(report.payload_bytes_sent) / report.elapsed_seconds
                                          : 0.0;
            report.gigabytes_per_second = report.bytes_per_second / 1'000'000'000.0;
            report.gibibytes_per_second = report.bytes_per_second / (1024.0 * 1024.0 * 1024.0);
            report.gigabits_per_second = report.bytes_per_second * 8.0 / 1'000'000'000.0;
            report.transports = transports;
            report.generator_threads = generator_threads;
            report.sender_threads = sender_threads;
            report.receiver_threads = 0;
            report.discarder_threads = 0;
            report.buffer_size = buffer_size;
            report.pool_slots_per_transport = pool_slots_per_transport;
            report.buffers_per_transport = buffers_per_transport;
            report.pattern = to_string(generator_pattern);
            report.transport_kind = "tcp-remote-sender-sharded";
            return report;
        }

        struct SendLane {
            RawBufferPool pool;
            BufferPoolRegistry registry;
            BufQueue queue;
            std::unique_ptr<BufferSenderJob> sender;
            std::unique_ptr<BufferGeneratorJob> generator;

            SendLane(std::size_t pool_slots, std::size_t bytes_per_buffer)
                : pool(kDataBufferPoolId, pool_slots, bytes_per_buffer),
                  queue(pool_slots) {
                registry.register_pool(pool);
            }
        };

        std::vector<std::unique_ptr<SendLane>> lanes;
        lanes.reserve(transports);
        for (std::size_t index = 0; index < transports; ++index) {
            auto lane = std::make_unique<SendLane>(pool_slots_per_transport, buffer_size);
            lane->sender = std::make_unique<BufferSenderJob>(
                sender_threads,
                lane->queue,
                lane->registry,
                BufferTransportEndpoint::tcp(tcp_host, static_cast<std::uint16_t>(base_port + index)));
            lane->generator = std::make_unique<BufferGeneratorJob>(
                BufferGeneratorConfig(generator_threads,
                                      buffers_per_transport,
                                      generator_pattern,
                                      0x9e3779b97f4a7c15ULL ^ static_cast<std::uint64_t>(index),
                                      1.0),
                lane->pool,
                lane->queue);
            lanes.push_back(std::move(lane));
        }

        const auto started_at = std::chrono::steady_clock::now();
        for (auto& lane : lanes) {
            lane->sender->start();
        }
        for (auto& lane : lanes) {
            lane->generator->start();
        }
        for (auto& lane : lanes) {
            lane->generator->wait();
        }
        for (auto& lane : lanes) {
            lane->sender->wait();
        }
        const auto ended_at = std::chrono::steady_clock::now();

        BufferTransportBenchmarkReport report;
        for (const auto& lane : lanes) {
            const auto generator_stats = lane->generator->stats();
            const auto sender_stats = lane->sender->stats();
            report.buffers_generated += generator_stats.buffers_generated;
            report.payload_bytes_sent += generator_stats.bytes_generated;
            report.buffers_sent += sender_stats.buffers;
        }
        report.elapsed_seconds = std::chrono::duration<double>(ended_at - started_at).count();
        report.bytes_per_second = report.elapsed_seconds > 0.0
                                      ? static_cast<double>(report.payload_bytes_sent) / report.elapsed_seconds
                                      : 0.0;
        report.gigabytes_per_second = report.bytes_per_second / 1'000'000'000.0;
        report.gibibytes_per_second = report.bytes_per_second / (1024.0 * 1024.0 * 1024.0);
        report.gigabits_per_second = report.bytes_per_second * 8.0 / 1'000'000'000.0;
        report.transports = transports;
        report.generator_threads = generator_threads;
        report.sender_threads = sender_threads;
        report.receiver_threads = 0;
        report.discarder_threads = 0;
        report.buffer_size = buffer_size;
        report.pool_slots_per_transport = pool_slots_per_transport;
        report.buffers_per_transport = buffers_per_transport;
        report.pattern = to_string(generator_pattern);
        report.transport_kind = "tcp-remote-sender";
        return report;
    }

    if (transport_kind == "none") {
        if (shared_input_queue) {
            const std::size_t total_pool_slots = pool_slots_per_transport * transports;
            const std::uint64_t total_buffers = buffers_per_transport * transports;
            const std::size_t shard_count = std::max<std::size_t>(generator_threads, discarder_threads);
            const std::size_t queue_depth_per_shard =
                std::max<std::size_t>(1U, (total_pool_slots + shard_count - 1U) / shard_count);
            RawBufferPool pool(kDataBufferPoolId, total_pool_slots, buffer_size);
            BufferPoolRegistry registry;
            registry.register_pool(pool);
            ShardedBufQueue queue(shard_count, queue_depth_per_shard);
            BufferDiscarderJob discarder(BufferDiscarderConfig(discarder_threads), queue, registry);
            BufferGeneratorJob generator(
                BufferGeneratorConfig(generator_threads,
                                      total_buffers,
                                      generator_pattern,
                                      0x9e3779b97f4a7c15ULL,
                                      1.0),
                pool,
                queue);

            const auto started_at = std::chrono::steady_clock::now();
            discarder.start();
            generator.start();
            generator.wait();
            discarder.wait();
            const auto ended_at = std::chrono::steady_clock::now();

            const auto generator_stats = generator.stats();
            const auto discarder_stats = discarder.stats();
            BufferTransportBenchmarkReport report;
            report.buffers_generated = generator_stats.buffers_generated;
            report.buffers_discarded = discarder_stats.buffers_discarded;
            report.payload_bytes_sent = generator_stats.bytes_generated;
            report.payload_bytes_received = discarder_stats.bytes_discarded;
            report.buffers_sent = report.buffers_generated;
            report.buffers_received = report.buffers_discarded;
            report.elapsed_seconds = std::chrono::duration<double>(ended_at - started_at).count();
            report.bytes_per_second = report.elapsed_seconds > 0.0
                                          ? static_cast<double>(report.payload_bytes_received) / report.elapsed_seconds
                                          : 0.0;
            report.gigabytes_per_second = report.bytes_per_second / 1'000'000'000.0;
            report.gibibytes_per_second = report.bytes_per_second / (1024.0 * 1024.0 * 1024.0);
            report.gigabits_per_second = report.bytes_per_second * 8.0 / 1'000'000'000.0;
            report.transports = shard_count;
            report.generator_threads = generator_threads;
            report.sender_threads = 0;
            report.receiver_threads = 0;
            report.discarder_threads = discarder_threads;
            report.buffer_size = buffer_size;
            report.pool_slots_per_transport = total_pool_slots;
            report.buffers_per_transport = total_buffers;
            report.pattern = to_string(generator_pattern);
            report.transport_kind = "none-sharded";
            return report;
        }

        struct LocalLane {
            RawBufferPool pool;
            BufferPoolRegistry registry;
            ShardedBufQueue queue;
            std::unique_ptr<BufferDiscarderJob> discarder;
            std::unique_ptr<BufferGeneratorJob> generator;

            LocalLane(BufferPoolId pool_id,
                      std::size_t pool_slots,
                      std::size_t bytes_per_buffer,
                      std::size_t queue_depth)
                : pool(pool_id, pool_slots, bytes_per_buffer),
                  queue(1U, queue_depth) {
                registry.register_pool(pool);
            }
        };

        std::vector<std::unique_ptr<LocalLane>> lanes;
        lanes.reserve(transports);
        const std::size_t queue_depth = std::max<std::size_t>(1U, pool_slots_per_transport);
        for (std::size_t index = 0; index < transports; ++index) {
            auto lane = std::make_unique<LocalLane>(kDataBufferPoolId,
                                                    pool_slots_per_transport,
                                                    buffer_size,
                                                    queue_depth);
            lane->discarder = std::make_unique<BufferDiscarderJob>(BufferDiscarderConfig(discarder_threads),
                                                                   lane->queue,
                                                                   lane->registry);
            lane->generator = std::make_unique<BufferGeneratorJob>(
                BufferGeneratorConfig(generator_threads,
                                      buffers_per_transport,
                                      generator_pattern,
                                      0x9e3779b97f4a7c15ULL ^ static_cast<std::uint64_t>(index),
                                      1.0),
                lane->pool,
                lane->queue);
            lanes.push_back(std::move(lane));
        }

        const auto started_at = std::chrono::steady_clock::now();
        for (auto& lane : lanes) {
            lane->discarder->start();
        }
        for (auto& lane : lanes) {
            lane->generator->start();
        }
        BufferTransportBenchmarkReport report;
        for (auto& lane : lanes) {
            lane->generator->wait();
        }
        for (auto& lane : lanes) {
            lane->discarder->wait();
        }
        const auto ended_at = std::chrono::steady_clock::now();

        for (const auto& lane : lanes) {
            const auto generator_stats = lane->generator->stats();
            const auto discarder_stats = lane->discarder->stats();
            report.buffers_generated += generator_stats.buffers_generated;
            report.buffers_discarded += discarder_stats.buffers_discarded;
            report.payload_bytes_sent += generator_stats.bytes_generated;
            report.payload_bytes_received += discarder_stats.bytes_discarded;
        }
        report.buffers_sent = report.buffers_generated;
        report.buffers_received = report.buffers_discarded;
        report.elapsed_seconds = std::chrono::duration<double>(ended_at - started_at).count();
        report.bytes_per_second = report.elapsed_seconds > 0.0
                                      ? static_cast<double>(report.payload_bytes_received) / report.elapsed_seconds
                                      : 0.0;
        report.gigabytes_per_second = report.bytes_per_second / 1'000'000'000.0;
        report.gibibytes_per_second = report.bytes_per_second / (1024.0 * 1024.0 * 1024.0);
        report.gigabits_per_second = report.bytes_per_second * 8.0 / 1'000'000'000.0;
        report.transports = transports;
        report.generator_threads = generator_threads;
        report.sender_threads = 0;
        report.receiver_threads = 0;
        report.discarder_threads = discarder_threads;
        report.buffer_size = buffer_size;
        report.pool_slots_per_transport = pool_slots_per_transport;
        report.buffers_per_transport = buffers_per_transport;
        report.pattern = to_string(generator_pattern);
        report.transport_kind = transport_kind;
        return report;
    }

    if (shared_input_queue) {
        const std::size_t total_pool_slots = pool_slots_per_transport * transports;
        const std::uint64_t total_buffers = buffers_per_transport * transports;
        RawBufferPool send_pool(kDataBufferPoolId, total_pool_slots, buffer_size);
        BufferPoolRegistry send_registry;
        send_registry.register_pool(send_pool);
        const std::size_t queue_depth_per_lane =
            std::max<std::size_t>(1U, pool_slots_per_transport);
        ShardedBufQueue sender_input(transports, queue_depth_per_lane);
        BufferGeneratorJob generator(BufferGeneratorConfig(generator_threads,
                                                           total_buffers,
                                                           generator_pattern,
                                                           0x9e3779b97f4a7c15ULL,
                                                           1.0),
                                     send_pool,
                                     sender_input);

        std::vector<std::unique_ptr<RawBufferPool>> receive_pools;
        std::vector<std::unique_ptr<BufferPoolRegistry>> receive_registries;
        std::vector<std::unique_ptr<BufQueue>> receiver_outputs;
        std::vector<std::unique_ptr<BufferReceiverJob>> receivers;
        std::vector<std::unique_ptr<BufferDiscarderJob>> discarders;
        std::vector<std::unique_ptr<BufferSenderJob>> senders;
        receive_pools.reserve(transports);
        receive_registries.reserve(transports);
        receiver_outputs.reserve(transports);
        receivers.reserve(transports);
        discarders.reserve(transports);
        senders.reserve(transports);

        for (std::size_t index = 0; index < transports; ++index) {
            BufferTransportEndpoint endpoint =
                transport_kind == "unix"
                    ? BufferTransportEndpoint::unix_socket(effective_socket_dir /
                                                           ("transport-" + std::to_string(index) + ".sock"))
                    : BufferTransportEndpoint::tcp("127.0.0.1", static_cast<std::uint16_t>(base_port + index));
            receive_pools.push_back(std::make_unique<RawBufferPool>(kDataBufferPoolId,
                                                                     pool_slots_per_transport,
                                                                     buffer_size));
            receive_registries.push_back(std::make_unique<BufferPoolRegistry>());
            receive_registries.back()->register_pool(*receive_pools.back());
            receiver_outputs.push_back(std::make_unique<BufQueue>(std::max<std::size_t>(1U, pool_slots_per_transport)));
            receivers.push_back(std::make_unique<BufferReceiverJob>(receiver_threads,
                                                                    *receive_pools.back(),
                                                                    *receiver_outputs.back(),
                                                                    endpoint));
            discarders.push_back(std::make_unique<BufferDiscarderJob>(BufferDiscarderConfig(discarder_threads),
                                                                      *receiver_outputs.back(),
                                                                      *receive_registries.back()));
            senders.push_back(std::make_unique<BufferSenderJob>(sender_threads,
                                                                sender_input.shard(index),
                                                                send_registry,
                                                                std::move(endpoint)));
        }

        const auto started_at = std::chrono::steady_clock::now();
        for (std::size_t index = 0; index < transports; ++index) {
            receivers[index]->start();
            discarders[index]->start();
        }
        for (auto& sender : senders) {
            sender->start();
        }
        generator.start();

        generator.wait();
        for (auto& sender : senders) {
            sender->wait();
        }
        for (auto& receiver : receivers) {
            receiver->wait();
        }
        for (auto& discarder : discarders) {
            discarder->wait();
        }
        const auto ended_at = std::chrono::steady_clock::now();

        BufferTransportBenchmarkReport report;
        const auto generator_stats = generator.stats();
        report.buffers_generated = generator_stats.buffers_generated;
        report.payload_bytes_sent = generator_stats.bytes_generated;
        for (std::size_t index = 0; index < transports; ++index) {
            const auto sender_stats = senders[index]->stats();
            const auto receiver_stats = receivers[index]->stats();
            const auto discarder_stats = discarders[index]->stats();
            report.buffers_sent += sender_stats.buffers;
            report.buffers_received += receiver_stats.buffers;
            report.buffers_discarded += discarder_stats.buffers_discarded;
            report.payload_bytes_received += receiver_stats.payload_bytes;
        }
        report.elapsed_seconds = std::chrono::duration<double>(ended_at - started_at).count();
        report.bytes_per_second = report.elapsed_seconds > 0.0
                                      ? static_cast<double>(report.payload_bytes_received) / report.elapsed_seconds
                                      : 0.0;
        report.gigabytes_per_second = report.bytes_per_second / 1'000'000'000.0;
        report.gibibytes_per_second = report.bytes_per_second / (1024.0 * 1024.0 * 1024.0);
        report.gigabits_per_second = report.bytes_per_second * 8.0 / 1'000'000'000.0;
        report.transports = transports;
        report.generator_threads = generator_threads;
        report.sender_threads = sender_threads;
        report.receiver_threads = receiver_threads;
        report.discarder_threads = discarder_threads;
        report.buffer_size = buffer_size;
        report.pool_slots_per_transport = pool_slots_per_transport;
        report.buffers_per_transport = buffers_per_transport;
        report.pattern = to_string(generator_pattern);
        report.transport_kind = transport_kind + "-sharded";
        if (transport_kind == "unix" && socket_dir.empty()) {
            std::error_code ignored;
            std::filesystem::remove_all(effective_socket_dir, ignored);
        }
        return report;
    }

    struct TransportLane {
        RawBufferPool send_pool;
        RawBufferPool receive_pool;
        BufferPoolRegistry send_registry;
        BufferPoolRegistry receive_registry;
        BufQueue generator_to_sender;
        BufQueue receiver_to_discarder;
        BufferTransportEndpoint endpoint;
        std::unique_ptr<BufferReceiverJob> receiver;
        std::unique_ptr<BufferDiscarderJob> discarder;
        std::unique_ptr<BufferSenderJob> sender;
        std::unique_ptr<BufferGeneratorJob> generator;

        TransportLane(BufferPoolId pool_id,
                      std::size_t pool_slots,
                      std::size_t bytes_per_buffer,
                      std::size_t queue_depth,
                      BufferTransportEndpoint lane_endpoint)
            : send_pool(pool_id, pool_slots, bytes_per_buffer),
              receive_pool(pool_id, pool_slots, bytes_per_buffer),
              generator_to_sender(queue_depth),
              receiver_to_discarder(queue_depth),
              endpoint(std::move(lane_endpoint)) {
            send_registry.register_pool(send_pool);
            receive_registry.register_pool(receive_pool);
        }
    };

    std::vector<std::unique_ptr<TransportLane>> lanes;
    lanes.reserve(transports);
    const std::size_t queue_depth = std::max<std::size_t>(1U, pool_slots_per_transport);
    for (std::size_t index = 0; index < transports; ++index) {
        BufferTransportEndpoint endpoint =
            transport_kind == "unix"
                ? BufferTransportEndpoint::unix_socket(effective_socket_dir /
                                                       ("transport-" + std::to_string(index) + ".sock"))
                : BufferTransportEndpoint::tcp("127.0.0.1", static_cast<std::uint16_t>(base_port + index));
        auto lane = std::make_unique<TransportLane>(kDataBufferPoolId,
                                                    pool_slots_per_transport,
                                                    buffer_size,
                                                    queue_depth,
                                                    std::move(endpoint));
        lane->receiver = std::make_unique<BufferReceiverJob>(receiver_threads,
                                                             lane->receive_pool,
                                                             lane->receiver_to_discarder,
                                                             lane->endpoint);
        lane->discarder = std::make_unique<BufferDiscarderJob>(
            BufferDiscarderConfig(discarder_threads),
            lane->receiver_to_discarder,
            lane->receive_registry);
        lane->sender = std::make_unique<BufferSenderJob>(sender_threads,
                                                         lane->generator_to_sender,
                                                         lane->send_registry,
                                                         lane->endpoint);
        lane->generator = std::make_unique<BufferGeneratorJob>(
            BufferGeneratorConfig(generator_threads,
                                  buffers_per_transport,
                                  generator_pattern,
                                  0x9e3779b97f4a7c15ULL ^ static_cast<std::uint64_t>(index),
                                  1.0),
            lane->send_pool,
            lane->generator_to_sender);
        lanes.push_back(std::move(lane));
    }

    const auto started_at = std::chrono::steady_clock::now();
    for (auto& lane : lanes) {
        lane->receiver->start();
        lane->discarder->start();
    }
    for (auto& lane : lanes) {
        lane->sender->start();
    }
    for (auto& lane : lanes) {
        lane->generator->start();
    }

    BufferTransportBenchmarkReport report;
    for (auto& lane : lanes) {
        lane->generator->wait();
    }
    for (auto& lane : lanes) {
        lane->sender->wait();
    }
    for (auto& lane : lanes) {
        lane->receiver->wait();
    }
    for (auto& lane : lanes) {
        lane->discarder->wait();
    }
    const auto ended_at = std::chrono::steady_clock::now();

    for (const auto& lane : lanes) {
        const auto generator_stats = lane->generator->stats();
        const auto sender_stats = lane->sender->stats();
        const auto receiver_stats = lane->receiver->stats();
        const auto discarder_stats = lane->discarder->stats();
        report.buffers_generated += generator_stats.buffers_generated;
        report.buffers_sent += sender_stats.buffers;
        report.buffers_received += receiver_stats.buffers;
        report.buffers_discarded += discarder_stats.buffers_discarded;
        report.payload_bytes_sent += sender_stats.payload_bytes;
        report.payload_bytes_received += receiver_stats.payload_bytes;
    }
    report.elapsed_seconds = std::chrono::duration<double>(ended_at - started_at).count();
    report.bytes_per_second = report.elapsed_seconds > 0.0
                                  ? static_cast<double>(report.payload_bytes_received) / report.elapsed_seconds
                                  : 0.0;
    report.gigabytes_per_second = report.bytes_per_second / 1'000'000'000.0;
    report.gibibytes_per_second = report.bytes_per_second / (1024.0 * 1024.0 * 1024.0);
    report.gigabits_per_second = report.bytes_per_second * 8.0 / 1'000'000'000.0;
    report.transports = transports;
    report.generator_threads = generator_threads;
    report.sender_threads = sender_threads;
    report.receiver_threads = receiver_threads;
    report.discarder_threads = discarder_threads;
    report.buffer_size = buffer_size;
    report.pool_slots_per_transport = pool_slots_per_transport;
    report.buffers_per_transport = buffers_per_transport;
    report.pattern = to_string(generator_pattern);
    report.transport_kind = transport_kind;

    if (transport_kind == "unix" && socket_dir.empty()) {
        std::error_code ignored;
        std::filesystem::remove_all(effective_socket_dir, ignored);
    }
    return report;
}

FakeRemoteDiffBenchmarkReport TransferEngine::benchmark_fake_remote_diff_pipeline(
    std::uint64_t file_count,
    std::uint64_t folder_count,
    std::uint64_t average_file_size,
    std::size_t source_threads,
    std::size_t fake_remote_threads,
    std::uint64_t remote_delay_microseconds,
    std::size_t request_queue_depth,
    std::size_t batch_queue_depth,
    std::uint32_t stats_interval_seconds,
    std::size_t checker_threads) const {
    const CheckerConfig checker_config = load_checker_config(config_store_);
    const std::size_t checker_thread_count =
        checker_threads != 0U ? checker_threads : checker_config.worker_count;
    const std::size_t effective_request_queue_depth =
        request_queue_depth != 0U ? request_queue_depth : checker_config.target_request_queue_depth;
    const std::size_t effective_batch_queue_depth =
        batch_queue_depth != 0U ? batch_queue_depth : checker_config.batch_queue_depth;

    if (folder_count == 0U ||
        source_threads == 0U ||
        fake_remote_threads == 0U ||
        checker_thread_count == 0U ||
        effective_request_queue_depth == 0U ||
        effective_batch_queue_depth == 0U) {
        throw std::invalid_argument("fake remote diff benchmark numeric parameters must be positive");
    }

    DiffTargetFolderQueue target_queue;
    target_queue.max_entries = effective_request_queue_depth;
    const std::size_t batch_queue_depth_per_checker =
        std::max<std::size_t>(1024U, (effective_batch_queue_depth + checker_thread_count - 1U) / checker_thread_count);
    DiffBatchQueueShards source_batches =
        make_diff_batch_queue_shards(checker_thread_count, source_threads, batch_queue_depth_per_checker);
    DiffBatchQueueShards target_batches =
        make_diff_batch_queue_shards(checker_thread_count, fake_remote_threads, batch_queue_depth_per_checker);
    FakeRemoteProcessorQueue fake_processor_queue;
    configure_fake_remote_processor_queue(fake_processor_queue,
                                          fake_remote_threads,
                                          effective_request_queue_depth);

    TransferReport diff_report;
    diff_report.mode = Mode::dry_run;
    std::mutex report_mutex;
    std::atomic<std::uint64_t> next_folder{0};
    std::atomic<std::uint64_t> source_files_generated{0};
    std::atomic<std::uint64_t> source_folders_generated{0};
    std::atomic<std::uint64_t> source_bytes_generated{0};
    std::atomic<std::uint64_t> target_folders_checked{0};
    std::atomic<bool> source_done{false};
    std::atomic<bool> stats_done{false};
    DiffPipelineTimingCounters timing;
    std::vector<std::unique_ptr<std::atomic<bool>>> joiner_pending_empty;
    joiner_pending_empty.reserve(checker_thread_count);
    for (std::size_t index = 0; index < checker_thread_count; ++index) {
        joiner_pending_empty.push_back(std::make_unique<std::atomic<bool>>(true));
    }

    const auto started_at = std::chrono::steady_clock::now();
    std::thread stats_thread;
    if (stats_interval_seconds != 0U) {
        stats_thread = std::thread([&]() {
            std::uint64_t last_source_files = 0;
            std::uint64_t last_compared_files = 0;
            auto last_at = started_at;
            while (!stats_done.load(std::memory_order_relaxed)) {
                std::this_thread::sleep_for(std::chrono::seconds(stats_interval_seconds));
                if (stats_done.load(std::memory_order_relaxed)) {
                    break;
                }
                std::uint64_t compared = 0;
                std::uint64_t same = 0;
                {
                    std::lock_guard<std::mutex> lock(report_mutex);
                    compared = diff_report.files_total;
                    same = diff_report.files_skipped;
                }
                const std::uint64_t source_files = source_files_generated.load(std::memory_order_relaxed);
                const auto now = std::chrono::steady_clock::now();
                const double elapsed = std::chrono::duration<double>(now - started_at).count();
                const double interval_elapsed = std::chrono::duration<double>(now - last_at).count();
                const double source_rate = interval_elapsed > 0.0
                                               ? static_cast<double>(source_files - last_source_files) / interval_elapsed
                                               : 0.0;
                const double compare_rate = interval_elapsed > 0.0
                                                ? static_cast<double>(compared - last_compared_files) / interval_elapsed
                                                : 0.0;
                std::cout << "fake_diff_stats"
                          << " source_records_per_second=" << source_rate
                          << " compared_records_per_second=" << compare_rate
                          << " source_files=" << source_files
                          << " compared_files=" << compared
                          << " same=" << same
                          << " target_folders_checked="
                          << target_folders_checked.load(std::memory_order_relaxed)
                          << " source_done=" << (source_done.load(std::memory_order_relaxed) ? "true" : "false")
                          << " elapsed_seconds=" << elapsed << std::endl;
                last_source_files = source_files;
                last_compared_files = compared;
                last_at = now;
            }
        });
    }

    std::vector<std::thread> remote_receivers;
    remote_receivers.reserve(fake_remote_threads);
    for (std::size_t index = 0; index < fake_remote_threads; ++index) {
        remote_receivers.emplace_back(fake_remote_request_receiver_worker,
                                      std::ref(target_queue),
                                      std::ref(fake_processor_queue),
                                      std::ref(target_batches),
                                      &timing);
    }

    std::vector<std::thread> fake_processors;
    fake_processors.reserve(fake_remote_threads);
    for (std::size_t index = 0; index < fake_remote_threads; ++index) {
        fake_processors.emplace_back(fake_remote_processor_worker,
                                     file_count,
                                     folder_count,
                                     average_file_size,
                                     remote_delay_microseconds,
                                     std::ref(fake_processor_queue),
                                     std::ref(target_batches),
                                     std::ref(target_folders_checked),
                                     &timing);
    }

    std::vector<std::thread> checker_workers;
    checker_workers.reserve(checker_thread_count);
    for (std::size_t index = 0; index < checker_thread_count; ++index) {
        checker_workers.emplace_back(summary_diff_joiner_worker,
                                     std::string("size"),
                                     false,
                                     false,
                                     std::ref(target_queue),
                                     std::ref(*source_batches[index]),
                                     std::ref(*target_batches[index]),
                                     std::ref(diff_report),
                                     std::ref(report_mutex),
                                     &timing,
                                     joiner_pending_empty[index].get());
    }
    std::thread target_closer(close_diff_target_input_when_ready,
                              std::ref(target_queue),
                              std::ref(source_batches),
                              std::ref(target_batches),
                              std::cref(joiner_pending_empty));

    std::vector<std::thread> source_workers;
    source_workers.reserve(source_threads);
    for (std::size_t index = 0; index < source_threads; ++index) {
        source_workers.emplace_back(synthetic_diff_source_worker,
                                    file_count,
                                    folder_count,
                                    average_file_size,
                                    std::ref(next_folder),
                                    std::ref(target_queue),
                                    std::ref(source_batches),
                                    std::ref(source_files_generated),
                                    std::ref(source_folders_generated),
                                    std::ref(source_bytes_generated),
                                    &timing);
    }

    for (auto& worker : source_workers) {
        worker.join();
    }
    const auto source_ended_at = std::chrono::steady_clock::now();
    source_done.store(true, std::memory_order_relaxed);

    if (target_closer.joinable()) {
        target_closer.join();
    }
    for (auto& worker : remote_receivers) {
        worker.join();
    }
    for (auto& worker : fake_processors) {
        worker.join();
    }
    for (auto& worker : checker_workers) {
        worker.join();
    }
    const auto ended_at = std::chrono::steady_clock::now();

    stats_done.store(true, std::memory_order_relaxed);
    if (stats_thread.joinable()) {
        stats_thread.join();
    }

    if (target_queue.error) {
        std::rethrow_exception(target_queue.error);
    }
    if (fake_processor_queue.error) {
        std::rethrow_exception(fake_processor_queue.error);
    }
    for (const auto& queue : source_batches) {
        if (queue->error) {
            std::rethrow_exception(queue->error);
        }
    }
    for (const auto& queue : target_batches) {
        if (queue->error) {
            std::rethrow_exception(queue->error);
        }
    }

    FakeRemoteDiffBenchmarkReport report;
    report.source_files_generated = source_files_generated.load(std::memory_order_relaxed);
    report.source_folders_generated = source_folders_generated.load(std::memory_order_relaxed);
    report.target_folders_checked = target_folders_checked.load(std::memory_order_relaxed);
    report.bytes_compared = source_bytes_generated.load(std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lock(report_mutex);
        report.files_compared = diff_report.files_total;
        report.files_same = diff_report.files_skipped;
    }
    report.source_elapsed_seconds = std::chrono::duration<double>(source_ended_at - started_at).count();
    report.total_elapsed_seconds = std::chrono::duration<double>(ended_at - started_at).count();
    report.source_records_per_second = report.source_elapsed_seconds > 0.0
                                           ? static_cast<double>(report.source_files_generated) /
                                                 report.source_elapsed_seconds
                                           : 0.0;
    report.total_records_per_second = report.total_elapsed_seconds > 0.0
                                          ? static_cast<double>(report.files_compared) /
                                                report.total_elapsed_seconds
                                          : 0.0;
    report.source_threads = source_threads;
    report.fake_remote_threads = fake_remote_threads;
    report.checker_threads = checker_thread_count;
    report.remote_delay_microseconds = remote_delay_microseconds;
    report.request_queue_depth = effective_request_queue_depth;
    report.batch_queue_depth = effective_batch_queue_depth;
    report.source_wait_target_queue_seconds =
        ns_to_seconds(timing.source_wait_target_queue_ns.load(std::memory_order_relaxed));
    report.source_wait_batch_queue_seconds =
        ns_to_seconds(timing.source_wait_batch_queue_ns.load(std::memory_order_relaxed));
    report.fake_remote_wait_request_seconds =
        ns_to_seconds(timing.fake_remote_wait_request_ns.load(std::memory_order_relaxed));
    report.fake_remote_wait_processor_queue_seconds =
        ns_to_seconds(timing.fake_remote_wait_processor_queue_ns.load(std::memory_order_relaxed));
    report.fake_remote_delay_seconds =
        ns_to_seconds(timing.fake_remote_delay_ns.load(std::memory_order_relaxed));
    report.fake_remote_wait_batch_queue_seconds =
        ns_to_seconds(timing.fake_remote_wait_batch_queue_ns.load(std::memory_order_relaxed));
    report.joiner_idle_seconds = ns_to_seconds(timing.joiner_idle_ns.load(std::memory_order_relaxed));
    report.joiner_process_seconds = ns_to_seconds(timing.joiner_process_ns.load(std::memory_order_relaxed));
    return report;
}

DistributedDiffRunReport TransferEngine::run_bulk_manifest_diff_source(
    const std::filesystem::path& source_root,
    const std::string& target_host,
    std::uint16_t target_port,
    const std::filesystem::path& folder_report_path,
    const std::string& compare_mode,
    bool recursive,
    std::size_t meta_reader_threads,
    std::size_t metadata_async_depth,
    double max_duration_seconds,
    std::uint32_t stats_interval_seconds) const {
    const auto started_at = std::chrono::steady_clock::now();
    NfsMetaReaderConfig reader_config = load_nfs_meta_reader_config(config_store_);
    if (meta_reader_threads != 0U) {
        reader_config.worker_count = meta_reader_threads;
    }
    if (metadata_async_depth != 0U) {
        reader_config.async_directory_depth = metadata_async_depth;
    }
    const std::size_t worker_count = std::max<std::size_t>(1U, reader_config.worker_count);
    const std::size_t async_depth = std::max<std::size_t>(1U, reader_config.async_directory_depth);

    ScopedFd fd = connect_tcp(target_host, target_port, 60, 250);
    DistributedDiffSettings settings;
    settings.compare_mode = compare_mode;
    settings.recursive = recursive;
    settings.allow_target_only = true;
    write_distributed_diff_frame(fd.get(), DistributedDiffFrameType::config, serialize_distributed_diff_settings(settings));

    DataReadFileQueue file_queue;
    file_queue.max_entries = std::max<std::size_t>(262144U, worker_count * async_depth * 32U);
    DataReadBenchmarkStats stats;
    const std::optional<std::chrono::steady_clock::time_point> stop_at =
        max_duration_seconds > 0.0
            ? std::optional<std::chrono::steady_clock::time_point>(
                  started_at + std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                   std::chrono::duration<double>(max_duration_seconds)))
            : std::nullopt;

    std::exception_ptr scan_error;
    std::thread scanner([&]() {
        try {
            scan_metadata_to_file_queue(source_root.string(),
                                        recursive,
                                        worker_count,
                                        async_depth,
                                        reader_config.readdirplus_page_bytes,
                                        file_queue,
                                        stats,
                                        stop_at);
        } catch (...) {
            scan_error = std::current_exception();
            fail_data_file_work(file_queue, scan_error);
        }
    });

    DistributedDiffRunReport report;
    std::uint64_t source_files_sent = 0;
    std::uint64_t source_bytes_sent = 0;
    std::string manifest;
    manifest.reserve(kBulkManifestPayloadBytes);
    std::uint64_t manifests_sent = 0;
    auto flush_manifest = [&]() {
        if (manifest.empty()) {
            return;
        }
        write_distributed_diff_frame(fd.get(), DistributedDiffFrameType::source_manifest, manifest);
        ++manifests_sent;
        manifest.clear();
    };

    auto last_report = started_at;
    while (true) {
        std::vector<FileSpec> files = take_data_file_work_batch(file_queue, 4096U);
        if (files.empty()) {
            break;
        }
        for (const FileSpec& file : files) {
            if (manifest.size() + kBulkManifestTokenBytes > kBulkManifestPayloadBytes) {
                flush_manifest();
            }
            append_bulk_manifest_token(manifest, file);
            ++source_files_sent;
            source_bytes_sent += file_spec_logical_size(file);
        }
        const auto now = std::chrono::steady_clock::now();
        if (stats_interval_seconds != 0U &&
            std::chrono::duration<double>(now - last_report).count() >= stats_interval_seconds) {
            const double elapsed = std::chrono::duration<double>(now - started_at).count();
            std::cerr << "bulk_manifest_diff_source_progress"
                      << " files_sent=" << source_files_sent
                      << " manifests_sent=" << manifests_sent
                      << " files_per_second="
                      << (elapsed > 0.0 ? static_cast<double>(source_files_sent) / elapsed : 0.0)
                      << " elapsed_seconds=" << elapsed << '\n';
            last_report = now;
        }
    }
    flush_manifest();
    if (scanner.joinable()) {
        scanner.join();
    }
    if (scan_error) {
        std::rethrow_exception(scan_error);
    }
    if (file_queue.error) {
        std::rethrow_exception(file_queue.error);
    }

    write_distributed_diff_frame(fd.get(), DistributedDiffFrameType::source_done, {});
    auto summary_frame = read_distributed_diff_frame(fd.get());
    if (!summary_frame.has_value()) {
        throw std::runtime_error("bulk manifest diff target closed before summary");
    }
    if (summary_frame->type == DistributedDiffFrameType::error) {
        throw std::runtime_error(summary_frame->payload);
    }
    if (summary_frame->type != DistributedDiffFrameType::bulk_summary) {
        throw std::runtime_error("bulk manifest diff target returned unexpected frame");
    }
    const DistributedFolderDiffSummary summary = deserialize_folder_diff_summary(summary_frame->payload);
    merge_distributed_diff_summary(report, summary);
    report.folders_sent = manifests_sent;
    report.source_logical_size_bytes = source_bytes_sent;
    report.elapsed_seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started_at).count();
    report.files_per_second =
        report.elapsed_seconds > 0.0 ? static_cast<double>(report.files_compared) / report.elapsed_seconds : 0.0;
    report.folders_per_second =
        report.elapsed_seconds > 0.0 ? static_cast<double>(manifests_sent) / report.elapsed_seconds : 0.0;

    std::ofstream out(folder_report_path);
    if (!out) {
        throw std::runtime_error("failed to open bulk manifest diff report: " + folder_report_path.string());
    }
    write_folder_diff_csv_header(out);
    write_folder_diff_csv_row(out, summary);
    return report;
}

DistributedDiffRunReport TransferEngine::run_distributed_diff_source(
    const std::filesystem::path& source_root,
    const std::string& target_host,
    std::uint16_t target_port,
    const std::filesystem::path& folder_report_path,
    const std::string& compare_mode,
    bool recursive,
    std::size_t meta_reader_threads,
    std::size_t metadata_async_depth,
    double max_duration_seconds,
    std::uint32_t stats_interval_seconds,
    bool pipeline_autoscale,
    std::string autoscale_profile,
    std::filesystem::path autoscale_settings_path,
    std::uint64_t autoscale_interval_ms) const {
    const auto started_at = std::chrono::steady_clock::now();
    NfsMetaReaderConfig reader_config = load_nfs_meta_reader_config(config_store_);
    if (meta_reader_threads != 0U) {
        reader_config.worker_count = meta_reader_threads;
    }
    if (metadata_async_depth != 0U) {
        reader_config.async_directory_depth = metadata_async_depth;
    }
    const std::size_t worker_count = std::max<std::size_t>(1U, reader_config.worker_count);
    const std::size_t async_depth = std::max<std::size_t>(1U, reader_config.async_directory_depth);

    // Flat-folder payloads may need thousands of 1 MiB batches for a single
    // huge directory, so keep that window large. Result/control buffers are
    // intentionally smaller; they are not on the large-folder completion path
    // and they should apply backpressure before the process hoards memory.
    const std::size_t flat_slots = distributed_diff_flat_slots(worker_count, async_depth);
    const std::size_t control_slots = distributed_diff_control_slots(worker_count);
    RawBufferPool send_pool = make_metadata_batch_buffer_pool(flat_slots);
    RawBufferPool result_pool = make_metadata_batch_buffer_pool(control_slots);
    RawBufferPool folder_pool =
        make_folder_work_buffer_pool(kFolderWorkBufferPoolId, folder_work_slots(worker_count, async_depth));
    RawBufferPool folder_feedback_pool =
        make_folder_work_buffer_pool(kFolderFeedbackBufferPoolId, folder_work_slots(worker_count, async_depth));
    BufQueue send_queue(flat_slots);
    BufQueue result_queue(control_slots);
    BufQueue folder_queue(folder_pool.capacity());
    BufQueue folder_feedback_queue(folder_feedback_pool.capacity());
    BufferPoolRegistry send_registry;
    send_registry.register_pool(send_pool);

    ScopedFd fd = connect_tcp(target_host, target_port, 200, 50);
    BufferStreamReceiverJob result_receiver(1U, result_pool, result_queue, fd.get());
    DiffResultReportWriterJob result_writer(result_queue, result_pool, folder_report_path);
    BufferStreamSenderJob source_sender(1U,
                                        send_queue,
                                        send_registry,
                                        fd.get(),
                                        metadata_batch_payload_size);
    FolderSeederJob folder_seeder(recursive,
                                  max_duration_seconds,
                                  folder_pool,
                                  folder_feedback_pool,
                                  folder_queue,
                                  folder_feedback_queue);
    NfsMetaReaderBufferJob scanner(source_root.string(),
                                   compare_mode,
                                   worker_count,
                                   async_depth,
                                   folder_pool,
                                   folder_feedback_pool,
                                   folder_queue,
                                   folder_feedback_queue,
                                   send_pool,
                                   send_queue,
                                   max_duration_seconds);

    std::unique_ptr<AutoScaleProfileStore> autoscale_profile_store;
    std::unique_ptr<JobAutoScaleRunner> scanner_autoscaler;
    auto source_scanner_learned_workers = std::make_shared<std::atomic<std::size_t>>(scanner.active_worker_limit());
    if (pipeline_autoscale) {
        if (autoscale_profile.empty()) {
            autoscale_profile = "distributed_diff_source";
        }
        if (autoscale_settings_path.empty()) {
            autoscale_settings_path = default_autoscale_settings_path();
        }
        autoscale_profile_store = std::make_unique<AutoScaleProfileStore>(autoscale_settings_path);
        AutoScalePolicy policy = autoscale_profile_store->job_policy(autoscale_profile,
                                                                     "source_scanner",
                                                                     scanner.worker_count());
        policy.scale_up_input_fullness = 0.0;
        policy.scale_down_input_fullness = 0.01;
        policy.output_blocked_fullness = 0.95;
        policy.scale_up_output_fullness_limit = 0.80;
        policy.busy_scale_up = 0.20;
        policy.idle_scale_down = 0.80;
        policy.min_improvement_ratio = 0.02;
        policy.cooldown_samples = 1;
        policy.max_cooldown_samples = std::max<std::uint64_t>(
            1U,
            (5000U + std::max<std::uint64_t>(1U, autoscale_interval_ms) - 1U) /
                std::max<std::uint64_t>(1U, autoscale_interval_ms));
        auto scanner_samples =
            std::make_shared<std::deque<std::pair<std::chrono::steady_clock::time_point, std::uint64_t>>>();
        scanner_autoscaler = std::make_unique<JobAutoScaleRunner>(
            scanner,
            policy,
            [&scanner, &send_queue, scanner_samples] {
                const auto now = std::chrono::steady_clock::now();
                const DistributedDiffRunReport stats = scanner.stats();
                scanner_samples->emplace_back(now, stats.source_logical_size_bytes);
                while (scanner_samples->size() > 2U &&
                       std::chrono::duration<double>(now - scanner_samples->front().first).count() > 5.0) {
                    scanner_samples->pop_front();
                }
                AutoScaleMetrics metrics;
                metrics.input_fullness = 1.0;
                metrics.input_available_ratio = 1.0;
                metrics.output_fullness = static_cast<double>(send_queue.size()) /
                    static_cast<double>(std::max<std::size_t>(1U, send_queue.capacity()));
                const RuntimeMetricsSnapshot runtime = scanner.runtime_metrics().snapshot();
                if (runtime.total_wall_ns != 0U) {
                    if (metrics.output_fullness < 0.95) {
                        metrics.busy_ratio = 1.0;
                    } else {
                        const std::uint64_t useful_ns =
                            runtime.state_wall_ns[runtime_state_index(RuntimeState::processing)] +
                            runtime.state_wall_ns[runtime_state_index(RuntimeState::wait_io)];
                        metrics.busy_ratio = static_cast<double>(useful_ns) / static_cast<double>(runtime.total_wall_ns);
                    }
                    metrics.wait_output_ratio =
                        static_cast<double>(runtime.state_wall_ns[runtime_state_index(RuntimeState::wait_output_full)]) /
                        static_cast<double>(runtime.total_wall_ns);
                    metrics.wait_input_ratio =
                        static_cast<double>(runtime.state_wall_ns[runtime_state_index(RuntimeState::wait_input_empty)]) /
                        static_cast<double>(runtime.total_wall_ns);
                }
                if (scanner_samples->size() >= 2U) {
                    const auto& oldest = scanner_samples->front();
                    const double elapsed = std::chrono::duration<double>(now - oldest.first).count();
                    metrics.throughput_per_second =
                        elapsed > 0.0 ? static_cast<double>(stats.source_logical_size_bytes - oldest.second) / elapsed
                                      : 0.0;
                }
                return metrics;
            },
            std::chrono::milliseconds(std::max<std::uint64_t>(100U, autoscale_interval_ms)));
        source_scanner_learned_workers->store(scanner_autoscaler->active_workers(), std::memory_order_relaxed);
        scanner_autoscaler->set_decision_callback([source_scanner_learned_workers](const AutoScaleDecision& decision) {
            source_scanner_learned_workers->store(decision.active_workers, std::memory_order_relaxed);
            if (decision.changed) {
                std::cerr << "autoscale job=source_scanner active_workers="
                          << decision.active_workers
                          << " reason=" << decision.reason << '\n';
            }
        });
    }

    StatusRegistry status_registry;
    std::unique_ptr<PeriodicStatusReporter> status_reporter;
    if (stats_interval_seconds != 0U) {
        status_registry.register_job("source_scanner", [&scanner]() {
            const DistributedDiffRunReport stats = scanner.stats();
            MonitorJobSnapshot snapshot;
            snapshot.name = "source_scanner";
            snapshot.running = scanner.running();
            snapshot.worker_count = scanner.worker_count();
            snapshot.processed_count = stats.folders_sent;
            snapshot.byte_count = stats.source_logical_size_bytes;
            snapshot.count_unit = "folders";
            snapshot.has_runtime_metrics = true;
            snapshot.runtime_metrics = scanner.runtime_metrics().snapshot();
            snapshot.detail = "files_seen=" + std::to_string(stats.files_compared);
            return snapshot;
        });
        status_registry.register_job("source_sender", [&source_sender]() {
            const BufferTransportStats stats = source_sender.stats();
            MonitorJobSnapshot snapshot;
            snapshot.name = "source_sender";
            snapshot.running = source_sender.running();
            snapshot.worker_count = source_sender.worker_count();
            snapshot.processed_count = stats.buffers;
            snapshot.byte_count = stats.payload_bytes;
            snapshot.count_unit = "buffers";
            snapshot.has_runtime_metrics = true;
            snapshot.runtime_metrics = source_sender.runtime_metrics().snapshot();
            return snapshot;
        });
        status_registry.register_job("result_receiver", [&result_receiver]() {
            const BufferTransportStats stats = result_receiver.stats();
            MonitorJobSnapshot snapshot;
            snapshot.name = "result_receiver";
            snapshot.running = result_receiver.running();
            snapshot.worker_count = result_receiver.worker_count();
            snapshot.processed_count = stats.buffers;
            snapshot.byte_count = stats.payload_bytes;
            snapshot.count_unit = "buffers";
            snapshot.has_runtime_metrics = true;
            snapshot.runtime_metrics = result_receiver.runtime_metrics().snapshot();
            return snapshot;
        });
        status_registry.register_job("result_writer", [&result_writer]() {
            const DistributedDiffRunReport stats = result_writer.report();
            MonitorJobSnapshot snapshot;
            snapshot.name = "result_writer";
            snapshot.running = result_writer.running();
            snapshot.worker_count = result_writer.worker_count();
            snapshot.processed_count = stats.folders_reported;
            snapshot.byte_count = stats.files_compared;
            snapshot.count_unit = "folders";
            snapshot.has_runtime_metrics = true;
            snapshot.runtime_metrics = result_writer.runtime_metrics().snapshot();
            snapshot.detail = "files_compared=" + std::to_string(stats.files_compared) +
                              " same=" + std::to_string(stats.files_same) +
                              " changed=" + std::to_string(stats.files_changed) +
                              " new=" + std::to_string(stats.files_new) +
                              " failed=" + std::to_string(stats.files_failed);
            return snapshot;
        });
        status_registry.register_queue("source_send_queue", [&send_queue]() {
            return monitor_buf_queue("source_send_queue", send_queue);
        });
        status_registry.register_queue("result_queue", [&result_queue]() {
            return monitor_buf_queue("result_queue", result_queue);
        });
        status_reporter = std::make_unique<PeriodicStatusReporter>(
            status_registry,
            std::chrono::seconds(stats_interval_seconds),
            [](std::string status) {
                std::cout << status << std::flush;
            });
    }

    std::atomic<bool> stats_done{false};
    std::thread stats_thread;
    if (stats_interval_seconds != 0U) {
        stats_thread = std::thread([&] {
            std::uint64_t last_folders = 0;
            std::uint64_t last_files = 0;
            auto last_at = started_at;
            while (!stats_done.load(std::memory_order_relaxed)) {
                for (std::uint32_t tick = 0; tick < stats_interval_seconds * 10U; ++tick) {
                    if (stats_done.load(std::memory_order_relaxed)) {
                        break;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
                if (stats_done.load(std::memory_order_relaxed)) {
                    break;
                }
                DistributedDiffRunReport snapshot = result_writer.report();
                snapshot.folders_sent = scanner.stats().folders_sent;
                print_distributed_diff_source_stats(snapshot, started_at, last_folders, last_files, last_at);
                last_folders = snapshot.folders_reported;
                last_files = snapshot.files_compared;
                last_at = std::chrono::steady_clock::now();
            }
        });
    }

    bool scanner_autoscale_saved = false;
    auto finish_scanner_autoscale = [&] {
        if (!scanner_autoscaler || scanner_autoscale_saved) {
            return;
        }
        scanner_autoscaler->stop();
        const std::size_t learned_workers = source_scanner_learned_workers->load(std::memory_order_relaxed);
        if (autoscale_profile_store) {
            autoscale_profile_store->update_learned_workers(autoscale_profile,
                                                            "source_scanner",
                                                            learned_workers);
            autoscale_profile_store->save();
        }
        scanner_autoscale_saved = true;
        // Autoscaling can park workers that are waiting on empty input queues.
        // Wake every worker before wait() so normal shutdown never depends on
        // the currently learned active-worker limit.
        scanner.set_active_worker_limit(scanner.worker_count());
    };

    result_receiver.start();
    result_writer.start();
    folder_seeder.start();
    source_sender.start();
    scanner.start();
    if (scanner_autoscaler) {
        scanner_autoscaler->start();
    }
    if (status_reporter) {
        status_reporter->start();
    }

    std::exception_ptr wait_error;
    try {
        scanner.wait();
        folder_seeder.wait();
        if (auto error = folder_seeder.error()) {
            std::rethrow_exception(error);
        }
        source_sender.wait();
        result_receiver.wait();
        result_writer.wait();
    } catch (...) {
        wait_error = std::current_exception();
        try {
            finish_scanner_autoscale();
            scanner.stop();
        } catch (...) {}
        try {
            folder_seeder.stop();
        } catch (...) {}
        try {
            source_sender.stop();
        } catch (...) {}
        try {
            result_receiver.stop();
        } catch (...) {}
        try {
            result_writer.stop();
        } catch (...) {}
    }
    finish_scanner_autoscale();

    stats_done.store(true, std::memory_order_relaxed);
    if (stats_thread.joinable()) {
        stats_thread.join();
    }
    if (status_reporter) {
        status_reporter->stop();
        std::cout << status_registry.render_human() << std::flush;
    }
    if (wait_error) {
        std::rethrow_exception(wait_error);
    }

    DistributedDiffRunReport report = result_writer.report();
    report.folders_sent = scanner.stats().folders_sent;
    report.elapsed_seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started_at).count();
    report.folders_per_second =
        report.elapsed_seconds > 0.0 ? static_cast<double>(report.folders_reported) / report.elapsed_seconds : 0.0;
    report.files_per_second =
        report.elapsed_seconds > 0.0 ? static_cast<double>(report.files_compared) / report.elapsed_seconds : 0.0;
    return report;
}

void TransferEngine::run_bulk_manifest_diff_target(const std::filesystem::path& target_root,
                                                   const std::string& listen_host,
                                                   std::uint16_t listen_port,
                                                   const std::string& compare_mode,
                                                   bool recursive,
                                                   std::size_t target_threads,
                                                   std::size_t metadata_async_depth,
                                                   std::uint32_t stats_interval_seconds) const {
    const auto started_at = std::chrono::steady_clock::now();
    NfsMetaReaderConfig reader_config = load_nfs_meta_reader_config(config_store_);
    const std::size_t worker_count =
        std::max<std::size_t>(1U, target_threads != 0U ? target_threads : 8U);
    const std::size_t async_depth =
        std::max<std::size_t>(1U, metadata_async_depth != 0U ? metadata_async_depth : reader_config.async_directory_depth);

    ScopedFd listener = listen_tcp(listen_host, listen_port, static_cast<int>(worker_count + 1U));
    std::cout << "bulk_manifest_diff_target_listening host=" << listen_host
              << " port=" << listen_port
              << " target=" << target_root.string()
              << " threads=" << worker_count
              << " metadata_async_depth=" << async_depth << std::endl;
    ScopedFd fd = accept_tcp(listener.get());

    auto settings_frame = read_distributed_diff_frame(fd.get());
    if (!settings_frame.has_value() || settings_frame->type != DistributedDiffFrameType::config) {
        throw std::runtime_error("bulk manifest diff target expected config frame");
    }
    DistributedDiffSettings settings = deserialize_distributed_diff_settings(settings_frame->payload);
    if (compare_mode != "size-time") {
        settings.compare_mode = compare_mode;
    }
    settings.recursive = recursive;

    BulkTargetStateMap state(512U);
    DataReadFileQueue target_files;
    target_files.max_entries = 262144U;
    DataReadBenchmarkStats scan_stats;
    std::exception_ptr scan_error;
    std::thread scanner([&]() {
        try {
            scan_metadata_to_file_queue(target_root.string(),
                                        settings.recursive,
                                        worker_count,
                                        async_depth,
                                        reader_config.readdirplus_page_bytes,
                                        target_files,
                                        scan_stats);
        } catch (...) {
            scan_error = std::current_exception();
            fail_data_file_work(target_files, scan_error);
        }
    });

    std::vector<std::thread> builders;
    builders.reserve(worker_count);
    for (std::size_t index = 0; index < worker_count; ++index) {
        builders.emplace_back([&]() {
            while (true) {
                std::vector<FileSpec> files = take_data_file_work_batch(target_files, 4096U);
                if (files.empty()) {
                    break;
                }
                for (const FileSpec& file : files) {
                    state.insert(file);
                }
            }
        });
    }
    if (scanner.joinable()) {
        scanner.join();
    }
    for (std::thread& builder : builders) {
        builder.join();
    }
    if (scan_error) {
        std::rethrow_exception(scan_error);
    }
    if (target_files.error) {
        std::rethrow_exception(target_files.error);
    }

    DistributedFolderDiffSummary summary;
    summary.rel_path = "";
    summary.target_scan_started_unix_ns = now_unix_ns();
    summary.target_file_count = state.files.load(std::memory_order_relaxed);
    summary.target_logical_size_bytes = state.logical_size_bytes.load(std::memory_order_relaxed);
    summary.target_scan_finished_unix_ns = now_unix_ns();

    std::uint64_t manifests_received = 0;
    auto last_report = std::chrono::steady_clock::now();
    try {
        while (true) {
            auto frame = read_distributed_diff_frame(fd.get());
            if (!frame.has_value()) {
                throw std::runtime_error("bulk manifest diff source closed before source_done");
            }
            if (frame->type == DistributedDiffFrameType::source_done) {
                break;
            }
            if (frame->type != DistributedDiffFrameType::source_manifest) {
                throw std::runtime_error("bulk manifest diff target received unexpected frame");
            }
            if (frame->payload.size() % kBulkManifestTokenBytes != 0U) {
                throw std::runtime_error("bulk manifest payload has a partial token");
            }
            ++manifests_received;
            std::size_t offset = 0;
            while (offset < frame->payload.size()) {
                const BulkManifestToken token = read_bulk_manifest_token(frame->payload, offset);
                ++summary.source_file_count;
                summary.source_logical_size_bytes += token.size;
                const std::optional<BulkTargetState> target = state.mark_seen(token.path_hash);
                if (!target.has_value()) {
                    ++summary.files_new;
                    summary.new_logical_size_bytes += token.size;
                    summary.bytes_planned += token.size;
                } else if (bulk_manifest_token_matches(token, *target, settings.compare_mode)) {
                    ++summary.files_same;
                    summary.same_logical_size_bytes += token.size;
                } else {
                    ++summary.files_changed;
                    summary.changed_logical_size_bytes += token.size;
                    summary.bytes_planned += token.size;
                }
            }
            const auto now = std::chrono::steady_clock::now();
            if (stats_interval_seconds != 0U &&
                std::chrono::duration<double>(now - last_report).count() >= stats_interval_seconds) {
                const double elapsed = std::chrono::duration<double>(now - started_at).count();
                std::cout << "bulk_manifest_diff_target_stats"
                          << " manifests_received=" << manifests_received
                          << " source_files=" << summary.source_file_count
                          << " target_files=" << summary.target_file_count
                          << " same=" << summary.files_same
                          << " changed=" << summary.files_changed
                          << " new=" << summary.files_new
                          << " files_per_second="
                          << (elapsed > 0.0 ? static_cast<double>(summary.source_file_count) / elapsed : 0.0)
                          << " elapsed_seconds=" << elapsed << std::endl;
                last_report = now;
            }
        }
        const auto [target_only_count, target_only_bytes] = state.target_only_counts();
        summary.files_target_only = target_only_count;
        summary.target_only_logical_size_bytes = target_only_bytes;
        summary.result_sent_unix_ns = now_unix_ns();
        write_distributed_diff_frame(fd.get(),
                                     DistributedDiffFrameType::bulk_summary,
                                     serialize_folder_diff_summary(summary));
        write_distributed_diff_frame(fd.get(), DistributedDiffFrameType::target_done, {});
        const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - started_at).count();
        std::cout << "bulk_manifest_diff_target_done"
                  << " manifests_received=" << manifests_received
                  << " source_files=" << summary.source_file_count
                  << " target_files=" << summary.target_file_count
                  << " same=" << summary.files_same
                  << " changed=" << summary.files_changed
                  << " new=" << summary.files_new
                  << " target_only=" << summary.files_target_only
                  << " bytes_planned=" << summary.bytes_planned
                  << " elapsed_seconds=" << elapsed << std::endl;
    } catch (...) {
        try {
            const std::string message = current_exception_message();
            write_distributed_diff_frame(fd.get(), DistributedDiffFrameType::error, message);
        } catch (...) {
        }
        throw;
    }
}

void TransferEngine::run_distributed_diff_target(const std::filesystem::path& target_root,
                                                 const std::string& listen_host,
                                                 std::uint16_t listen_port,
                                                 const std::string& compare_mode,
                                                 bool recursive,
                                                 std::size_t target_threads,
                                                 std::size_t metadata_async_depth,
                                                 std::uint32_t stats_interval_seconds,
                                                 bool pipeline_autoscale,
                                                 std::string autoscale_profile,
                                                 std::filesystem::path autoscale_settings_path,
                                                 std::uint64_t autoscale_interval_ms) const {
    NfsMetaReaderConfig reader_config = load_nfs_meta_reader_config(config_store_);
    const std::size_t worker_count =
        std::max<std::size_t>(1U, target_threads != 0U ? target_threads : reader_config.worker_count);
    const std::size_t async_depth =
        std::max<std::size_t>(1U, metadata_async_depth != 0U ? metadata_async_depth : reader_config.async_directory_depth);

    ScopedFd listener = listen_tcp(listen_host, listen_port, static_cast<int>(worker_count + 1U));
    std::cout << "distributed_diff_target_listening host=" << listen_host
              << " port=" << listen_port
              << " target=" << target_root.string()
              << " threads=" << worker_count
              << " metadata_async_depth=" << async_depth << std::endl;
    ScopedFd fd = accept_tcp(listener.get());

    (void)recursive;
    const std::size_t shard_count = std::max<std::size_t>(1U, std::min<std::size_t>(worker_count, 128U));
    // Source/target flat-folder payloads are the only buffers that may need a
    // large completion window for very wide directories. Request/result buffers
    // are control-plane traffic, so keep them smaller to force backpressure to
    // the socket instead of retaining tens of GiB in userspace.
    const std::size_t flat_slots = distributed_diff_flat_slots(worker_count, async_depth);
    const std::size_t control_slots = distributed_diff_control_slots(worker_count);
    const std::size_t shard_depth = std::max<std::size_t>(64U, (flat_slots + shard_count - 1U) / shard_count);

    RawBufferPool source_pool = make_distributed_diff_metadata_pool(kDistributedDiffSourcePoolId, flat_slots);
    RawBufferPool request_pool = make_distributed_diff_metadata_pool(kDistributedDiffRequestPoolId, control_slots);
    RawBufferPool target_pool = make_distributed_diff_metadata_pool(kDistributedDiffTargetPoolId, flat_slots);
    RawBufferPool result_pool = make_distributed_diff_metadata_pool(kDistributedDiffResultPoolId, control_slots);
    BufQueue received_source_queue(flat_slots);
    BufQueue target_request_queue(control_slots);
    BufQueue result_queue(control_slots);
    std::vector<std::unique_ptr<BufQueue>> diff_shard_queues;
    diff_shard_queues.reserve(shard_count);
    for (std::size_t index = 0; index < shard_count; ++index) {
        diff_shard_queues.push_back(std::make_unique<BufQueue>(shard_depth * 2U));
    }

    BufferPoolRegistry result_registry;
    result_registry.register_pool(result_pool);
    BufferStreamReceiverJob source_receiver(1U, source_pool, received_source_queue, fd.get());
    SourceBatchRouterJob router(received_source_queue,
                                source_pool,
                                request_pool,
                                target_request_queue,
                                diff_shard_queues);
    TargetFolderScannerBufferJob target_scanner(target_root.string(),
                                                compare_mode,
                                                worker_count,
                                                async_depth,
                                                target_request_queue,
                                                request_pool,
                                                target_pool,
                                                diff_shard_queues);
    std::unique_ptr<AutoScaleProfileStore> autoscale_profile_store;
    std::unique_ptr<JobAutoScaleRunner> target_scanner_autoscaler;
    auto target_scanner_learned_workers = std::make_shared<std::atomic<std::size_t>>(target_scanner.active_worker_limit());
    if (pipeline_autoscale) {
        if (autoscale_profile.empty()) {
            autoscale_profile = "distributed_diff_target";
        }
        if (autoscale_settings_path.empty()) {
            autoscale_settings_path = default_autoscale_settings_path();
        }
        autoscale_profile_store = std::make_unique<AutoScaleProfileStore>(autoscale_settings_path);
        AutoScalePolicy policy = autoscale_profile_store->job_policy(autoscale_profile,
                                                                     "target_scanner",
                                                                     target_scanner.worker_count());
        policy.scale_up_input_fullness = 0.10;
        policy.scale_down_input_fullness = 0.02;
        policy.output_blocked_fullness = 0.95;
        policy.scale_up_output_fullness_limit = 0.80;
        policy.busy_scale_up = 0.20;
        policy.idle_scale_down = 0.80;
        policy.min_improvement_ratio = 0.02;
        policy.cooldown_samples = 1;
        policy.max_cooldown_samples = std::max<std::uint64_t>(
            1U,
            (5000U + std::max<std::uint64_t>(1U, autoscale_interval_ms) - 1U) /
                std::max<std::uint64_t>(1U, autoscale_interval_ms));
        auto target_scanner_samples =
            std::make_shared<std::deque<std::pair<std::chrono::steady_clock::time_point, std::uint64_t>>>();
        target_scanner_autoscaler = std::make_unique<JobAutoScaleRunner>(
            target_scanner,
            policy,
            [&target_scanner, &target_request_queue, &diff_shard_queues, target_scanner_samples] {
                const auto now = std::chrono::steady_clock::now();
                const TargetFolderScannerBufferJob::Stats stats = target_scanner.stats();
                target_scanner_samples->emplace_back(now, stats.logical_size_bytes);
                while (target_scanner_samples->size() > 2U &&
                       std::chrono::duration<double>(now - target_scanner_samples->front().first).count() > 5.0) {
                    target_scanner_samples->pop_front();
                }
                AutoScaleMetrics metrics;
                const double input_fullness = static_cast<double>(target_request_queue.size()) /
                    static_cast<double>(std::max<std::size_t>(1U, target_request_queue.capacity()));
                metrics.input_fullness = input_fullness;
                metrics.input_available_ratio =
                    target_request_queue.size() >= target_scanner.active_worker_limit()
                        ? 1.0
                        : static_cast<double>(target_request_queue.size()) /
                              static_cast<double>(std::max<std::size_t>(1U, target_scanner.active_worker_limit()));
                std::size_t output_size = 0;
                std::size_t output_capacity = 0;
                for (const auto& queue : diff_shard_queues) {
                    output_size += queue->size();
                    output_capacity += queue->capacity();
                }
                metrics.output_fullness = static_cast<double>(output_size) /
                    static_cast<double>(std::max<std::size_t>(1U, output_capacity));
                const RuntimeMetricsSnapshot runtime = target_scanner.runtime_metrics().snapshot();
                if (runtime.total_wall_ns != 0U) {
                    const bool has_backlog = target_request_queue.size() != 0U;
                    if (has_backlog && metrics.output_fullness < 0.95) {
                        metrics.busy_ratio = 1.0;
                    } else {
                        const std::uint64_t useful_ns =
                            runtime.state_wall_ns[runtime_state_index(RuntimeState::processing)] +
                            runtime.state_wall_ns[runtime_state_index(RuntimeState::wait_io)];
                        metrics.busy_ratio = static_cast<double>(useful_ns) / static_cast<double>(runtime.total_wall_ns);
                    }
                    metrics.wait_output_ratio =
                        static_cast<double>(runtime.state_wall_ns[runtime_state_index(RuntimeState::wait_output_full)]) /
                        static_cast<double>(runtime.total_wall_ns);
                    metrics.wait_input_ratio =
                        static_cast<double>(runtime.state_wall_ns[runtime_state_index(RuntimeState::wait_input_empty)]) /
                        static_cast<double>(runtime.total_wall_ns);
                }
                if (target_scanner_samples->size() >= 2U) {
                    const auto& oldest = target_scanner_samples->front();
                    const double elapsed = std::chrono::duration<double>(now - oldest.first).count();
                    metrics.throughput_per_second =
                        elapsed > 0.0 ? static_cast<double>(stats.logical_size_bytes - oldest.second) / elapsed : 0.0;
                }
                return metrics;
            },
            std::chrono::milliseconds(std::max<std::uint64_t>(100U, autoscale_interval_ms)));
        target_scanner_learned_workers->store(target_scanner_autoscaler->active_workers(), std::memory_order_relaxed);
        target_scanner_autoscaler->set_decision_callback([target_scanner_learned_workers](const AutoScaleDecision& decision) {
            target_scanner_learned_workers->store(decision.active_workers, std::memory_order_relaxed);
            if (decision.changed) {
                std::cerr << "autoscale job=target_scanner active_workers="
                          << decision.active_workers
                          << " reason=" << decision.reason << '\n';
            }
        });
    }
    std::vector<std::unique_ptr<FolderDiffShardJob>> diff_shards;
    diff_shards.reserve(shard_count);
    for (std::size_t index = 0; index < shard_count; ++index) {
        diff_shards.push_back(std::make_unique<FolderDiffShardJob>(*diff_shard_queues[index],
                                                                   source_pool,
                                                                   target_pool,
                                                                   result_pool,
                                                                   result_queue));
    }
    BufferStreamSenderJob result_sender(1U,
                                        result_queue,
                                        result_registry,
                                        fd.get(),
                                        metadata_batch_payload_size);
    StatusRegistry status_registry;
    std::unique_ptr<PeriodicStatusReporter> status_reporter;
    if (stats_interval_seconds != 0U) {
        status_registry.register_job("source_receiver", [&source_receiver]() {
            const BufferTransportStats stats = source_receiver.stats();
            MonitorJobSnapshot snapshot;
            snapshot.name = "source_receiver";
            snapshot.running = source_receiver.running();
            snapshot.worker_count = source_receiver.worker_count();
            snapshot.processed_count = stats.buffers;
            snapshot.byte_count = stats.payload_bytes;
            snapshot.count_unit = "buffers";
            snapshot.has_runtime_metrics = true;
            snapshot.runtime_metrics = source_receiver.runtime_metrics().snapshot();
            return snapshot;
        });
        status_registry.register_job("source_router", [&router]() {
            const SourceBatchRouterJob::Stats stats = router.stats();
            MonitorJobSnapshot snapshot;
            snapshot.name = "source_router";
            snapshot.running = router.running();
            snapshot.worker_count = router.worker_count();
            snapshot.processed_count = stats.source_batches_routed;
            snapshot.byte_count = stats.target_requests_sent;
            snapshot.count_unit = "buffers";
            snapshot.has_runtime_metrics = true;
            snapshot.runtime_metrics = router.runtime_metrics().snapshot();
            snapshot.detail = "target_requests=" + std::to_string(stats.target_requests_sent);
            return snapshot;
        });
        status_registry.register_job("target_scanner", [&target_scanner]() {
            const TargetFolderScannerBufferJob::Stats stats = target_scanner.stats();
            MonitorJobSnapshot snapshot;
            snapshot.name = "target_scanner";
            snapshot.running = target_scanner.running();
            snapshot.worker_count = target_scanner.worker_count();
            snapshot.processed_count = stats.folders_scanned;
            snapshot.byte_count = stats.logical_size_bytes;
            snapshot.count_unit = "folders";
            snapshot.has_runtime_metrics = true;
            snapshot.runtime_metrics = target_scanner.runtime_metrics().snapshot();
            snapshot.detail = "files_seen=" + std::to_string(stats.files_seen);
            return snapshot;
        });
        status_registry.register_job("diff_shards", [&diff_shards]() {
            FolderDiffShardJob::Stats totals;
            RuntimeMetricsSnapshot runtime;
            bool running = false;
            for (const auto& shard : diff_shards) {
                const FolderDiffShardJob::Stats stats = shard->stats();
                totals.folders_compared += stats.folders_compared;
                totals.files_compared += stats.files_compared;
                totals.bytes_planned += stats.bytes_planned;
                totals.source_batches += stats.source_batches;
                totals.target_batches += stats.target_batches;
                running = running || shard->running();
                add_runtime_metrics(runtime, shard->runtime_metrics().snapshot());
            }
            MonitorJobSnapshot snapshot;
            snapshot.name = "diff_shards";
            snapshot.running = running;
            snapshot.worker_count = diff_shards.size();
            snapshot.processed_count = totals.folders_compared;
            snapshot.byte_count = totals.bytes_planned;
            snapshot.count_unit = "folders";
            snapshot.has_runtime_metrics = true;
            snapshot.runtime_metrics = runtime;
            snapshot.detail = "files_compared=" + std::to_string(totals.files_compared) +
                              " source_batches=" + std::to_string(totals.source_batches) +
                              " target_batches=" + std::to_string(totals.target_batches);
            return snapshot;
        });
        status_registry.register_job("result_sender", [&result_sender]() {
            const BufferTransportStats stats = result_sender.stats();
            MonitorJobSnapshot snapshot;
            snapshot.name = "result_sender";
            snapshot.running = result_sender.running();
            snapshot.worker_count = result_sender.worker_count();
            snapshot.processed_count = stats.buffers;
            snapshot.byte_count = stats.payload_bytes;
            snapshot.count_unit = "buffers";
            snapshot.has_runtime_metrics = true;
            snapshot.runtime_metrics = result_sender.runtime_metrics().snapshot();
            return snapshot;
        });
        status_registry.register_queue("received_source_queue", [&received_source_queue]() {
            return monitor_buf_queue("received_source_queue", received_source_queue);
        });
        status_registry.register_queue("target_request_queue", [&target_request_queue]() {
            return monitor_buf_queue("target_request_queue", target_request_queue);
        });
        status_registry.register_queue("diff_shard_queues", [&diff_shard_queues]() {
            return monitor_queue_group("diff_shard_queues", diff_shard_queues);
        });
        status_registry.register_queue("result_queue", [&result_queue]() {
            return monitor_buf_queue("result_queue", result_queue);
        });
        status_reporter = std::make_unique<PeriodicStatusReporter>(
            status_registry,
            std::chrono::seconds(stats_interval_seconds),
            [](std::string status) {
                std::cout << status << std::flush;
            });
    }
    std::atomic<bool> stats_done{false};

    std::thread stats_thread;
    const auto started_at = std::chrono::steady_clock::now();
    if (stats_interval_seconds != 0U) {
        stats_thread = std::thread([&] {
            std::uint64_t last_folders = 0;
            std::uint64_t last_files = 0;
            auto last_at = started_at;
            while (!stats_done.load(std::memory_order_relaxed)) {
                for (std::uint32_t tick = 0; tick < stats_interval_seconds * 10U; ++tick) {
                    if (stats_done.load(std::memory_order_relaxed)) {
                        break;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
                if (stats_done.load(std::memory_order_relaxed)) {
                    break;
                }
                const BufferTransportStats rx = source_receiver.stats();
                const BufferTransportStats tx = result_sender.stats();
                std::uint64_t folders_compared = 0;
                std::uint64_t files_compared = 0;
                for (const auto& shard : diff_shards) {
                    const FolderDiffShardJob::Stats shard_stats = shard->stats();
                    folders_compared += shard_stats.folders_compared;
                    files_compared += shard_stats.files_compared;
                }
                const auto now = std::chrono::steady_clock::now();
                const double elapsed = std::chrono::duration<double>(now - started_at).count();
                const double interval_elapsed = std::chrono::duration<double>(now - last_at).count();
                const std::uint64_t interval_folders =
                    folders_compared >= last_folders ? folders_compared - last_folders : 0U;
                const std::uint64_t interval_files =
                    files_compared >= last_files ? files_compared - last_files : 0U;
                std::cout << "distributed_diff_target_stats"
                          << " source_buffers_received=" << rx.buffers
                          << " result_buffers_sent=" << tx.buffers
                          << " folders_compared=" << folders_compared
                          << " interval_folders_per_second="
                          << (interval_elapsed > 0.0 ? static_cast<double>(interval_folders) / interval_elapsed : 0.0)
                          << " files_compared=" << files_compared
                          << " files_per_second="
                          << (elapsed > 0.0 ? static_cast<double>(files_compared) / elapsed : 0.0)
                          << " interval_files_per_second="
                          << (interval_elapsed > 0.0 ? static_cast<double>(interval_files) / interval_elapsed : 0.0)
                          << " receive_queue_depth=" << received_source_queue.size()
                          << " request_queue_depth=" << target_request_queue.size()
                          << " result_queue_depth=" << result_queue.size()
                          << " elapsed_seconds=" << elapsed << std::endl;
                last_folders = folders_compared;
                last_files = files_compared;
                last_at = now;
            }
        });
    }

    bool target_scanner_autoscale_saved = false;
    auto finish_target_scanner_autoscale = [&] {
        if (!target_scanner_autoscaler || target_scanner_autoscale_saved) {
            return;
        }
        target_scanner_autoscaler->stop();
        const std::size_t learned_workers = target_scanner_learned_workers->load(std::memory_order_relaxed);
        if (autoscale_profile_store) {
            autoscale_profile_store->update_learned_workers(autoscale_profile,
                                                            "target_scanner",
                                                            learned_workers);
            autoscale_profile_store->save();
        }
        target_scanner_autoscale_saved = true;
        // See source-side shutdown: parked workers must be woken before wait().
        target_scanner.set_active_worker_limit(target_scanner.worker_count());
    };

    source_receiver.start();
    router.start();
    target_scanner.start();
    if (target_scanner_autoscaler) {
        target_scanner_autoscaler->start();
    }
    for (auto& shard : diff_shards) {
        shard->start();
    }
    result_sender.start();
    if (status_reporter) {
        status_reporter->start();
    }

    std::exception_ptr wait_error;
    try {
        source_receiver.wait();
        router.wait();
        finish_target_scanner_autoscale();
        target_scanner.wait();
        for (auto& queue : diff_shard_queues) {
            queue->close();
        }
        for (auto& shard : diff_shards) {
            shard->wait();
        }
        result_queue.close();
        result_sender.wait();
    } catch (...) {
        wait_error = std::current_exception();
        try {
            source_receiver.stop();
        } catch (...) {}
        try {
            router.stop();
        } catch (...) {}
        try {
            finish_target_scanner_autoscale();
            target_scanner.stop();
        } catch (...) {}
        for (auto& queue : diff_shard_queues) {
            queue->close();
        }
        for (auto& shard : diff_shards) {
            try {
                shard->stop();
            } catch (...) {}
        }
        result_queue.close();
        try {
            result_sender.stop();
        } catch (...) {}
    }
    finish_target_scanner_autoscale();

    stats_done.store(true, std::memory_order_relaxed);
    if (stats_thread.joinable()) {
        stats_thread.join();
    }
    if (status_reporter) {
        status_reporter->stop();
        std::cout << status_registry.render_human() << std::flush;
    }
    if (wait_error) {
        std::rethrow_exception(wait_error);
    }
}

TransferReport TransferEngine::run_copy_source_pipeline(const std::filesystem::path& source_root,
                                                        const std::string& target_host,
                                                        std::uint16_t base_port,
                                                        std::size_t lanes,
                                                        bool recursive,
                                                        std::size_t meta_reader_threads,
                                                        std::size_t metadata_async_depth,
                                                        std::size_t data_reader_threads,
                                                        std::size_t data_outstanding_requests,
                                                        std::size_t data_buffer_slots,
                                                        std::size_t lane_queue_depth,
                                                        bool pack_small_files,
                                                        double max_duration_seconds,
                                                        std::uint32_t stats_interval_seconds,
                                                        bool shared_nothing) const {
    if (source_root.empty()) {
        throw std::runtime_error("--source is required");
    }
    if (target_host.empty()) {
        throw std::runtime_error("--host is required");
    }
    lanes = std::max<std::size_t>(1U, lanes);

    NfsMetaReaderConfig meta_config = load_nfs_meta_reader_config(config_store_);
    meta_config.source_root = source_root.string();
    meta_config.recursive = recursive;
    if (meta_reader_threads != 0U) {
        meta_config.worker_count = meta_reader_threads;
    }
    if (metadata_async_depth != 0U) {
        meta_config.async_directory_depth = metadata_async_depth;
    }

    NfsDataReaderConfig data_config = load_nfs_data_reader_config(config_store_);
    data_config.source_root = source_root.string();
    data_config.copy_data_from_nfs = true;
    data_config.pack_small_files = pack_small_files;
    data_config.small_file_threshold = config_.small_file_threshold;
    data_config.large_chunk_bytes = config_.large_chunk_bytes;
    if (data_reader_threads != 0U) {
        data_config.data_reader_worker_count = data_reader_threads;
    }
    if (data_outstanding_requests != 0U) {
        data_config.outstanding_requests = data_outstanding_requests;
    }

    if (shared_nothing) {
        data_config.data_reader_worker_count = 1U;
        data_config.large_chunk_bytes = 1024U * 1024U;
    }
    const std::size_t effective_data_threads = std::max<std::size_t>(1U, data_config.data_reader_worker_count);
    const std::size_t effective_outstanding = std::max<std::size_t>(1U, data_config.outstanding_requests);
    lane_queue_depth = std::max<std::size_t>(1U, lane_queue_depth == 0U ? 1024U : lane_queue_depth);
    const std::size_t minimum_pool_slots =
        effective_data_threads * effective_outstanding + lanes * lane_queue_depth + lanes + 1U;
    const std::size_t pool_slots =
        std::max<std::size_t>(minimum_pool_slots,
                              data_buffer_slots == 0U ? minimum_pool_slots * 2U : data_buffer_slots);

    FlatMetadataWorkQueue folder_queue;
    folder_queue.folders.push_back(FileSpec{});
    DataReadFileQueue file_queue;
    file_queue.max_entries = std::max<std::size_t>(65536U, effective_data_threads * effective_outstanding * 8U);
    std::vector<std::unique_ptr<DataReadFileQueue>> lane_file_queues;
    if (shared_nothing) {
        lane_file_queues.reserve(lanes);
        for (std::size_t lane = 0; lane < lanes; ++lane) {
            auto lane_queue = std::make_unique<DataReadFileQueue>();
            lane_queue->max_entries = std::max<std::size_t>(lane_queue_depth, 65536U / lanes + 1U);
            lane_file_queues.push_back(std::move(lane_queue));
        }
    }

    DataReadBenchmarkStats stats;
    stats.print_interval_seconds = std::max<std::uint32_t>(1U, stats_interval_seconds);
    stats.folders_found.store(1U, std::memory_order_relaxed);
    reset_nfs_async_read_latency_metrics();

    RawBufferPool data_pool = make_data_buffer_pool(pool_slots);
    BufferPoolRegistry registry;
    registry.register_pool(data_pool);
    std::vector<ScopedFd> lane_fds(lanes);
    std::vector<std::exception_ptr> lane_connect_errors(lanes);
    std::vector<std::thread> lane_connectors;
    lane_connectors.reserve(lanes);
    for (std::size_t lane = 0; lane < lanes; ++lane) {
        lane_connectors.emplace_back([&, lane]() {
            try {
                lane_fds[lane] = connect_tcp(target_host, static_cast<std::uint16_t>(base_port + lane), 60, 250);
            } catch (...) {
                lane_connect_errors[lane] = std::current_exception();
            }
        });
    }
    for (std::thread& connector : lane_connectors) {
        connector.join();
    }
    for (const auto& error : lane_connect_errors) {
        if (error) {
            std::rethrow_exception(error);
        }
    }

    std::vector<std::unique_ptr<BufQueue>> lane_queues;
    std::vector<std::unique_ptr<BufferStreamSenderJob>> senders;
    if (!shared_nothing) {
        lane_queues.reserve(lanes);
        senders.reserve(lanes);
        for (std::size_t lane = 0; lane < lanes; ++lane) {
            lane_queues.push_back(std::make_unique<BufQueue>(lane_queue_depth));
            senders.push_back(std::make_unique<BufferStreamSenderJob>(
                1U,
                *lane_queues.back(),
                registry,
                lane_fds[lane].release()));
        }
    }

    const auto file_provider = [&file_queue]() {
        thread_local std::deque<FileSpec> worker_file_batch;
        if (worker_file_batch.empty()) {
            std::vector<FileSpec> next_batch = take_data_file_work_batch(file_queue, 128);
            for (auto& file : next_batch) {
                worker_file_batch.push_back(std::move(file));
            }
        }
        if (worker_file_batch.empty()) {
            return std::optional<FileSpec> {};
        }
        FileSpec file = std::move(worker_file_batch.front());
        worker_file_batch.pop_front();
        return std::optional<FileSpec> {std::move(file)};
    };
    const auto stop_predicate = [&file_queue]() {
        return data_read_timer_expired(file_queue);
    };
    const auto direct_send = [&](std::size_t, const BufferHandle& handle) {
        const DataBuffer& buffer = data_buffer(data_pool, handle);
        const std::uint64_t key = is_packed_small_file_buffer(buffer) && buffer.trailer.folder_hash != 0U
                                      ? buffer.trailer.folder_hash
                                      : buffer.trailer.file_id;
        const std::size_t lane = static_cast<std::size_t>(key % lanes);
        if (lane_queues[lane]->try_push(handle)) {
            return true;
        }
        return lane_queues[lane]->push_wait(handle);
    };

    const auto attach_reader_callbacks = [&stats](NfsDataBufferReaderJob& reader) {
        reader.set_bytes_read_callback([&stats](std::uint64_t bytes_read) {
            record_data_read_bytes(stats, bytes_read);
        });
        reader.set_file_read_callback([&stats]() {
            record_data_read_file(stats);
        });
        reader.set_file_failed_callback([&stats](const FileSpec&) {
            stats.files_failed.fetch_add(1U, std::memory_order_relaxed);
        });
    };

    std::unique_ptr<NfsDataBufferReaderJob> data_reader_job;
    std::vector<std::unique_ptr<NfsDataBufferReaderJob>> lane_readers;
    std::atomic<std::uint64_t> direct_sent_buffers {0};
    std::atomic<std::uint64_t> direct_sent_bytes {0};
    if (shared_nothing) {
        lane_readers.reserve(lanes);
        for (std::size_t lane = 0; lane < lanes; ++lane) {
            auto lane_provider = [&, lane]() {
                thread_local std::deque<FileSpec> worker_file_batch;
                if (worker_file_batch.empty()) {
                    std::vector<FileSpec> next_batch = take_data_file_work_batch(*lane_file_queues[lane], 128);
                    for (auto& file : next_batch) {
                        worker_file_batch.push_back(std::move(file));
                    }
                }
                if (worker_file_batch.empty()) {
                    return std::optional<FileSpec> {};
                }
                FileSpec file = std::move(worker_file_batch.front());
                worker_file_batch.pop_front();
                return std::optional<FileSpec> {std::move(file)};
            };
            auto lane_stop = [&, lane]() {
                return data_read_timer_expired(*lane_file_queues[lane]);
            };
            auto lane_send = [&, lane](std::size_t, const BufferHandle& handle) {
                const std::size_t payload_bytes = data_pool.buffer_size_bytes();
                const std::array<std::byte, 32> header = make_shared_nothing_header(payload_bytes);
                write_shared_nothing_frame(lane_fds[lane].get(), header, data_pool.data(handle), payload_bytes);
                data_pool.release(handle);
                direct_sent_buffers.fetch_add(1U, std::memory_order_relaxed);
                direct_sent_bytes.fetch_add(payload_bytes, std::memory_order_relaxed);
                return true;
            };
            NfsDataReaderConfig lane_config = data_config;
            lane_config.endpoint_index = lane;
            auto reader = std::make_unique<NfsDataBufferReaderJob>(lane_config,
                                                                   data_pool,
                                                                   NfsDataBufferReaderJob::BufferConsumer(lane_send),
                                                                   lane_provider,
                                                                   lane_stop);
            reader->set_interleave_file_provider([&, lane]() {
                return try_take_data_file_work(*lane_file_queues[lane]);
            });
            attach_reader_callbacks(*reader);
            lane_readers.push_back(std::move(reader));
        }
    } else {
        data_reader_job = std::make_unique<NfsDataBufferReaderJob>(data_config,
                                                                   data_pool,
                                                                   NfsDataBufferReaderJob::BufferConsumer(direct_send),
                                                                   file_provider,
                                                                   stop_predicate);
        attach_reader_callbacks(*data_reader_job);
    }

    const auto started_at = std::chrono::steady_clock::now();
    stats.started_at = started_at;
    stats.last_print_at = started_at;
    if (max_duration_seconds > 0.0) {
        const auto stop_at = started_at +
                             std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                 std::chrono::duration<double>(max_duration_seconds));
        folder_queue.stop_at = stop_at;
        file_queue.stop_at = stop_at;
    }

    std::thread stats_printer([&]() {
        std::uint64_t last_bytes = 0;
        std::uint64_t last_files = 0;
        auto last = started_at;
        while (!stats.printer_done.load(std::memory_order_acquire)) {
            std::unique_lock<std::mutex> lock(stats.printer_mutex);
            stats.printer_cv.wait_for(lock, std::chrono::seconds(stats.print_interval_seconds), [&]() {
                return stats.printer_done.load(std::memory_order_acquire);
            });
            const auto now = std::chrono::steady_clock::now();
            const double interval = std::chrono::duration<double>(now - last).count();
            if (interval <= 0.0) {
                continue;
            }
            NfsDataBufferReaderStats reader_snapshot;
            if (shared_nothing) {
                reader_snapshot.worker_count = lane_readers.size();
                for (const auto& reader : lane_readers) {
                    const auto lane_stats = reader->stats();
                    reader_snapshot.files_read += lane_stats.files_read;
                    reader_snapshot.files_failed += lane_stats.files_failed;
                    reader_snapshot.buffers_read += lane_stats.buffers_read;
                    reader_snapshot.bytes_read += lane_stats.bytes_read;
                }
            } else {
                reader_snapshot = data_reader_job->stats();
            }
            std::uint64_t sent_buffers = 0;
            std::uint64_t sent_bytes = 0;
            if (shared_nothing) {
                sent_buffers = direct_sent_buffers.load(std::memory_order_relaxed);
                sent_bytes = direct_sent_bytes.load(std::memory_order_relaxed);
            } else {
                for (const auto& sender : senders) {
                    const BufferTransportStats sender_stats = sender->stats();
                    sent_buffers += sender_stats.buffers;
                    sent_bytes += sender_stats.payload_bytes;
                }
            }
            std::cerr << "copy_source_progress"
                      << " files_found=" << stats.files_found.load(std::memory_order_relaxed)
                      << " files_read=" << reader_snapshot.files_read
                      << " files_failed=" << reader_snapshot.files_failed
                      << " read_gbit_s=" << (static_cast<double>(reader_snapshot.bytes_read - last_bytes) * 8.0 / interval / 1e9)
                      << " read_files_s=" << (static_cast<double>(reader_snapshot.files_read - last_files) / interval)
                      << " sent_buffers=" << sent_buffers
                      << " sent_gbit_total=" << (static_cast<double>(sent_bytes) * 8.0 / 1e9)
                      << '\n';
            last = now;
            last_bytes = reader_snapshot.bytes_read;
            last_files = reader_snapshot.files_read;
        }
    });

    std::mutex stop_timer_mutex;
    std::condition_variable stop_timer_cv;
    bool cancel_stop_timer = false;
    std::thread stop_timer;
    if (folder_queue.stop_at.has_value()) {
        const auto stop_at = *folder_queue.stop_at;
        stop_timer = std::thread([&]() {
            std::unique_lock<std::mutex> lock(stop_timer_mutex);
            const bool cancelled = stop_timer_cv.wait_until(lock, stop_at, [&]() {
                return cancel_stop_timer;
            });
            if (!cancelled) {
                request_flat_folder_stop(folder_queue);
                request_data_file_stop(file_queue);
                for (auto& queue : lane_file_queues) {
                    request_data_file_stop(*queue);
                }
            }
        });
    }

    for (auto& sender : senders) {
        sender->start();
    }
    if (shared_nothing) {
        for (auto& reader : lane_readers) {
            reader->start();
        }
    } else {
        data_reader_job->start();
    }

    std::vector<std::thread> metadata_workers;
    const std::size_t metadata_threads = std::max<std::size_t>(1U, meta_config.worker_count);
    metadata_workers.reserve(metadata_threads);
    for (std::size_t index = 0; index < metadata_threads; ++index) {
        if (shared_nothing) {
            metadata_workers.emplace_back(scan_data_read_metadata_worker_laned,
                                          meta_config.source_root,
                                          meta_config.recursive,
                                          std::max<std::size_t>(1U, meta_config.async_directory_depth),
                                          meta_config.readdirplus_page_bytes,
                                          std::ref(folder_queue),
                                          std::ref(lane_file_queues),
                                          std::ref(stats));
        } else {
            metadata_workers.emplace_back(scan_data_read_metadata_worker,
                                          meta_config.source_root,
                                          meta_config.recursive,
                                          std::max<std::size_t>(1U, meta_config.async_directory_depth),
                                          meta_config.readdirplus_page_bytes,
                                          0,
                                          0,
                                          std::ref(folder_queue),
                                          std::ref(file_queue),
                                          std::ref(stats),
                                          nullptr,
                                          nullptr);
        }
    }
    for (auto& worker : metadata_workers) {
        worker.join();
    }
    mark_data_file_input_done(file_queue);
    for (auto& queue : lane_file_queues) {
        mark_data_file_input_done(*queue);
    }

    std::exception_ptr pipeline_error;
    try {
        if (shared_nothing) {
            for (auto& reader : lane_readers) {
                reader->wait();
            }
            for (auto& fd : lane_fds) {
                if (fd.valid()) {
                    ::shutdown(fd.get(), SHUT_WR);
                }
            }
        } else {
            data_reader_job->wait();
        }
        for (auto& queue : lane_queues) {
            queue->close();
        }
        for (auto& sender : senders) {
            sender->wait();
        }
    } catch (...) {
        pipeline_error = std::current_exception();
        if (data_reader_job) {
            data_reader_job->stop();
        }
        for (auto& reader : lane_readers) {
            reader->stop();
        }
        for (auto& sender : senders) {
            sender->stop();
        }
    }

    stats.printer_done.store(true, std::memory_order_release);
    stats.printer_cv.notify_all();
    if (stats_printer.joinable()) {
        stats_printer.join();
    }
    {
        std::lock_guard<std::mutex> lock(stop_timer_mutex);
        cancel_stop_timer = true;
    }
    stop_timer_cv.notify_all();
    if (stop_timer.joinable()) {
        stop_timer.join();
    }
    if (folder_queue.error) {
        std::rethrow_exception(folder_queue.error);
    }
    if (file_queue.error) {
        std::rethrow_exception(file_queue.error);
    }
    for (const auto& queue : lane_file_queues) {
        if (queue->error) {
            std::rethrow_exception(queue->error);
        }
    }
    if (pipeline_error) {
        std::rethrow_exception(pipeline_error);
    }

    std::uint64_t sent_buffers = 0;
    if (shared_nothing) {
        sent_buffers = direct_sent_buffers.load(std::memory_order_relaxed);
    } else {
        for (const auto& sender : senders) {
            sent_buffers += sender->stats().buffers;
        }
    }
    NfsDataBufferReaderStats reader_stats;
    if (shared_nothing) {
        reader_stats.worker_count = lane_readers.size();
        for (const auto& reader : lane_readers) {
            const auto lane_stats = reader->stats();
            reader_stats.files_read += lane_stats.files_read;
            reader_stats.files_failed += lane_stats.files_failed;
            reader_stats.buffers_read += lane_stats.buffers_read;
            reader_stats.bytes_read += lane_stats.bytes_read;
        }
    } else {
        reader_stats = data_reader_job->stats();
    }
    TransferReport report;
    report.files_total = stats.files_found.load(std::memory_order_relaxed);
    report.files_transferred = reader_stats.files_read;
    report.files_failed = reader_stats.files_failed;
    report.bytes_transferred = reader_stats.bytes_read;
    report.chunks_sent = sent_buffers;
    report.elapsed_seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started_at).count();
    report.bytes_per_second =
        report.elapsed_seconds > 0.0 ? static_cast<double>(reader_stats.bytes_read) / report.elapsed_seconds : 0.0;
    report.pipeline_description =
        shared_nothing
            ? "[MetaReader-NFS-" + std::to_string(metadata_threads) +
                  "]->(FileQueue-parent-hash x" + std::to_string(lanes) +
                  ")->[DataReader-NFS-1 x" + std::to_string(lanes) +
                  "]->TCP[DirectSocketSender-1 x" + std::to_string(lanes) + "]"
            : "[MetaReader-NFS-" + std::to_string(metadata_threads) +
                  "]->(FileQueue)->[DataReader-NFS-" +
                  std::to_string(effective_data_threads) +
                  "]->(DataBufQueue-" + std::to_string(lane_queue_depth) +
                  " x" + std::to_string(lanes) + ")->[BufferStreamSender-1 x" +
                  std::to_string(lanes) + "]";
    return report;
}

TransferReport TransferEngine::run_copy_target_pipeline(const std::string& target_root,
                                                        const std::string& bind_host,
                                                        std::uint16_t base_port,
                                                        std::size_t lanes,
                                                        std::size_t data_buffer_slots_per_lane,
                                                        std::size_t lane_queue_depth,
                                                        bool verify_hash,
                                                        bool preserve_metadata,
                                                        bool target_fsync,
                                                        bool ensure_target_directories,
                                                        std::size_t writer_async_window,
                                                        std::size_t writer_file_window,
                                                        std::size_t writer_reactors,
                                                        std::size_t reactors_per_ip,
                                                        bool precreate_target_files,
                                                        bool shared_nothing) const {
    if (target_root.empty()) {
        throw std::runtime_error("--target is required");
    }
    lanes = std::max<std::size_t>(1U, lanes);
    lane_queue_depth = std::max<std::size_t>(1U, lane_queue_depth == 0U ? 1024U : lane_queue_depth);
    data_buffer_slots_per_lane = std::max<std::size_t>(lane_queue_depth + 2U,
                                                       data_buffer_slots_per_lane == 0U
                                                           ? lane_queue_depth * 2U + 2U
                                                           : data_buffer_slots_per_lane);
    if (ensure_target_directories && !shared_nothing) {
        const std::size_t receiver_pool_slots =
            std::max<std::size_t>(lanes * (data_buffer_slots_per_lane + 2U),
                                  lanes * lane_queue_depth + 8192U);
        RawBufferPool data_pool = make_data_buffer_pool(receiver_pool_slots);

        TargetDataWriterConfig base_writer_config = load_target_data_writer_config(config_store_);
        base_writer_config.target_root = target_root;
        base_writer_config.verify_hash = verify_hash;
        base_writer_config.preserve_metadata = preserve_metadata;
        base_writer_config.fsync_on_finish = target_fsync;
        base_writer_config.ensure_parent_directories = false;
        base_writer_config.assume_precreated_files = precreate_target_files;
        base_writer_config.reactors_per_ip = std::max<std::size_t>(1U, reactors_per_ip);
        base_writer_config.reactor_count = writer_reactors == 0U ? 64U : writer_reactors;
        base_writer_config.max_concurrent_file_transactions = writer_file_window == 0U ? 64U : writer_file_window;
        if (writer_async_window != 0U) {
            base_writer_config.async_window = writer_async_window;
        }

        TargetDataWriterConfig small_writer_config = base_writer_config;
        small_writer_config.worker_count = 64U;
        small_writer_config.async_window = writer_async_window == 0U ? 128U : writer_async_window;
        small_writer_config.direct_reactor_submit = false;
        small_writer_config.direct_reactor_writes = false;

        TargetDataWriterConfig medium_writer_config = base_writer_config;
        medium_writer_config.worker_count = 64U;
        medium_writer_config.async_window = 2U;
        medium_writer_config.direct_reactor_submit = false;
        medium_writer_config.direct_reactor_writes = false;

        TargetDataWriterConfig large_writer_config = base_writer_config;
        large_writer_config.worker_count = 48U;
        large_writer_config.async_window = 16U;
        large_writer_config.direct_reactor_submit = false;
        large_writer_config.direct_reactor_writes = false;

        const std::size_t small_threads = target_data_writer_effective_worker_count(small_writer_config);
        const std::size_t medium_threads = target_data_writer_effective_worker_count(medium_writer_config);
        const std::size_t large_threads = target_data_writer_effective_worker_count(large_writer_config);
        const std::size_t small_queue_depth_per_shard =
            std::max<std::size_t>(1U, (262144U + small_threads - 1U) / small_threads);
        const std::size_t target_bulk_queue_depth_per_shard = std::max<std::size_t>(lane_queue_depth, 16384U);
        const std::size_t file_create_threads = 16U;
        const std::size_t file_create_queue_depth_per_shard =
            std::max<std::size_t>(1U, (262144U + file_create_threads - 1U) / file_create_threads);
        ShardedBufQueue file_create_queue(file_create_threads, file_create_queue_depth_per_shard);
        ShardedBufQueue small_queue(small_threads, small_queue_depth_per_shard);
        ShardedBufQueue medium_queue(medium_threads, target_bulk_queue_depth_per_shard);
        ShardedBufQueue large_queue(large_threads, target_bulk_queue_depth_per_shard);

        CopyTargetEngineTelemetry telemetry;
        file_create_queue.set_depth_counter(&telemetry.file_create_queue_depth);
        small_queue.set_depth_counter(&telemetry.small_write_queue_depth);
        medium_queue.set_depth_counter(&telemetry.medium_write_queue_depth);
        large_queue.set_depth_counter(&telemetry.large_write_queue_depth);
        TargetBufferSpillway medium_spillway;
        TargetBufferSpillway large_spillway;
        medium_spillway.depth_counter = &telemetry.medium_spillway_size;
        large_spillway.depth_counter = &telemetry.large_spillway_size;
        std::atomic<std::uint64_t> folders_created {0};
        std::atomic<std::uint64_t> classifier_buffers {0};
        std::atomic<std::uint64_t> mkdir_calls {0};
        std::atomic<std::uint64_t> mkdir_wait_ns {0};
        std::atomic<std::uint64_t> small_buffers_routed {0};
        std::atomic<std::uint64_t> medium_buffers_routed {0};
        std::atomic<std::uint64_t> large_buffers_routed {0};
        std::atomic<bool> classifier_failed {false};
        std::atomic<bool> telemetry_done {false};
        std::mutex error_mutex;
        std::exception_ptr classifier_error;

        struct CopyTargetReceiverLane {
            BufQueue queue;
            std::unique_ptr<BufferReceiverJob> receiver;
            explicit CopyTargetReceiverLane(std::size_t depth) : queue(depth) {}
        };
        std::vector<std::unique_ptr<CopyTargetReceiverLane>> target_lanes;
        target_lanes.reserve(lanes);
        for (std::size_t lane = 0; lane < lanes; ++lane) {
            auto target_lane = std::make_unique<CopyTargetReceiverLane>(lane_queue_depth);
            target_lane->queue.set_depth_counter(&telemetry.rx_queue_depth);
            target_lane->receiver = std::make_unique<BufferReceiverJob>(
                1U,
                data_pool,
                target_lane->queue,
                BufferTransportEndpoint::tcp(bind_host.empty() ? std::string("0.0.0.0") : bind_host,
                                             static_cast<std::uint16_t>(base_port + lane)));
            target_lane->receiver->set_worker_cpu_affinity(lane % 8U, 1U);
            target_lanes.push_back(std::move(target_lane));
        }

        auto remember_classifier_error = [&](std::exception_ptr error) {
            {
                std::lock_guard<std::mutex> lock(error_mutex);
                if (!classifier_error) {
                    classifier_error = error;
                }
            }
            classifier_failed.store(true, std::memory_order_release);
            for (auto& lane : target_lanes) {
                lane->queue.close();
            }
            file_create_queue.close();
            small_queue.close();
            medium_queue.close();
            large_queue.close();
            fail_target_buffer_spillway(medium_spillway, data_pool, error);
            fail_target_buffer_spillway(large_spillway, data_pool, error);
        };

        const auto ensure_buffer_directories = [&](TargetWriterBackend& backend, const BufferHandle& handle) {
            const DataBuffer& buffer = data_buffer(data_pool, handle);
            std::vector<FileSpec> folders;
            std::unordered_set<std::string> seen;
            const auto add_parent = [&](std::string_view rel_path) {
                const std::string parent = parent_path(rel_path);
                if (parent.empty() || !seen.insert(parent).second) {
                    return;
                }
                FileSpec folder;
                folder.rel_path = parent;
                folder.mode = 0755U;
                folders.push_back(std::move(folder));
            };
            if (is_packed_small_file_buffer(buffer)) {
                const bool ok = visit_packed_small_files(buffer, [&](PackedSmallFileView view) {
                    add_parent(view.rel_path);
                });
                if (!ok) {
                    throw std::runtime_error("copy-target classifier received malformed packed-small-file buffer");
                }
            } else if (!buffer.trailer.rel_path.view().empty()) {
                add_parent(buffer.trailer.rel_path.view());
            }
            if (!folders.empty()) {
                const auto mkdir_started = std::chrono::steady_clock::now();
                const auto folder_count = static_cast<std::uint64_t>(folders.size());
                telemetry.mkdir_queue_depth.fetch_add(static_cast<std::int64_t>(folder_count),
                                                       std::memory_order_relaxed);
                telemetry.mkdir_requests_issued.fetch_add(folder_count, std::memory_order_relaxed);
                try {
                    backend.ensure_directories(folders);
                } catch (...) {
                    telemetry.mkdir_queue_depth.fetch_sub(static_cast<std::int64_t>(folder_count),
                                                           std::memory_order_relaxed);
                    throw;
                }
                const auto mkdir_finished = std::chrono::steady_clock::now();
                telemetry.mkdir_queue_depth.fetch_sub(static_cast<std::int64_t>(folder_count),
                                                       std::memory_order_relaxed);
                telemetry.mkdir_completions_ack.fetch_add(folder_count, std::memory_order_relaxed);
                mkdir_calls.fetch_add(1U, std::memory_order_relaxed);
                const auto mkdir_elapsed_ns = static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(mkdir_finished - mkdir_started).count());
                mkdir_wait_ns.fetch_add(mkdir_elapsed_ns, std::memory_order_relaxed);
                telemetry.mkdir_wait_ns.fetch_add(mkdir_elapsed_ns, std::memory_order_relaxed);
                folders_created.fetch_add(folders.size(), std::memory_order_relaxed);
            }
        };

        const auto push_sharded = [](ShardedBufQueue& queue, std::size_t shard, const BufferHandle& handle) {
            if (queue.try_push(shard, handle)) {
                return true;
            }
            return queue.push_wait(shard, handle);
        };
        const auto try_push_or_spill = [](ShardedBufQueue& queue,
                                          TargetBufferSpillway& spillway,
                                          std::size_t shard,
                                          const BufferHandle& handle) {
            if (queue.try_push(shard, handle)) {
                return true;
            }
            return spill_target_buffer(spillway, handle);
        };

        struct PrecreatedFileRegistry {
            std::mutex mutex;
            std::condition_variable cv;
            std::unordered_set<std::string> created;
            std::exception_ptr error;

            void mark_created(const std::string& rel_path) {
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    created.insert(rel_path);
                }
                cv.notify_all();
            }

            void fail(std::exception_ptr failure) {
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    if (!error) {
                        error = failure;
                    }
                }
                cv.notify_all();
            }

            void wait_created(const std::string& rel_path) {
                std::unique_lock<std::mutex> lock(mutex);
                cv.wait(lock, [&] {
                    return error || created.find(rel_path) != created.end();
                });
                if (error) {
                    std::rethrow_exception(error);
                }
            }
        };
        PrecreatedFileRegistry precreated_files;

        const auto create_buffer_files = [&](TargetWriterBackend& backend, const BufferHandle& handle) {
            const DataBuffer& buffer = data_buffer(data_pool, handle);
            std::vector<FileSpec> files;
            if (is_packed_small_file_buffer(buffer)) {
                files.reserve(packed_small_file_count(buffer));
                const bool ok = visit_packed_small_files(buffer, [&](PackedSmallFileView view) {
                    FileSpec file;
                    file.rel_path = std::string(view.rel_path);
                    file.declared_size = view.file_size;
                    file.mtime = view.mtime;
                    file.mode = view.mode != 0U ? view.mode : 0644U;
                    file.uid = view.uid;
                    file.gid = view.gid;
                    files.push_back(std::move(file));
                });
                if (!ok) {
                    throw std::runtime_error("copy-target precreation received malformed packed-small-file buffer");
                }
                backend.create_files(files);
                return;
            }

            FileSpec file = file_spec_from_data_trailer(buffer.trailer);
            if (file.rel_path.empty()) {
                throw std::runtime_error("copy-target precreation received a regular data buffer without a relative path");
            }
            const std::string rel_path = file.rel_path;
            if (buffer.trailer.data_offset == 0U) {
                files.push_back(std::move(file));
                backend.create_files(files);
                precreated_files.mark_created(rel_path);
            } else {
                precreated_files.wait_created(rel_path);
            }
        };

        const auto route_created_buffer = [&](const BufferHandle& handle) {
            const DataBuffer& buffer = data_buffer(data_pool, handle);
            if (is_packed_small_file_buffer(buffer)) {
                const std::uint64_t fallback_key = buffer.trailer.folder_hash != 0U
                                                       ? buffer.trailer.folder_hash
                                                       : (buffer.trailer.file_id != 0U
                                                              ? buffer.trailer.file_id
                                                              : static_cast<std::uint64_t>(handle.index));
                const std::uint64_t key = packed_small_parent_locality_hash(buffer, fallback_key);
                if (push_sharded(small_queue, static_cast<std::size_t>(key % small_threads), handle)) {
                    small_buffers_routed.fetch_add(1U, std::memory_order_relaxed);
                    return true;
                }
                return false;
            }

            const std::uint64_t file_size = buffer.trailer.file_size;
            const std::uint64_t key = buffer.trailer.file_id != 0U
                                          ? buffer.trailer.file_id
                                          : hash64(buffer.trailer.rel_path.view());
            if (file_size < 1024U * 1024U) {
                if (try_push_or_spill(medium_queue,
                                      medium_spillway,
                                      static_cast<std::size_t>(key % medium_threads),
                                      handle)) {
                    medium_buffers_routed.fetch_add(1U, std::memory_order_relaxed);
                    return true;
                }
                return false;
            }
            if (try_push_or_spill(large_queue,
                                  large_spillway,
                                  static_cast<std::size_t>(key % large_threads),
                                  handle)) {
                large_buffers_routed.fetch_add(1U, std::memory_order_relaxed);
                return true;
            }
            return false;
        };

        TargetDataWriterJob small_writer(small_writer_config, data_pool, small_queue);
        TargetDataWriterJob medium_writer(medium_writer_config, data_pool, medium_queue);
        TargetDataWriterJob large_writer(large_writer_config, data_pool, large_queue);
        medium_writer.set_secondary_input(small_queue, &telemetry.medium_to_small_steals);
        medium_writer.set_tertiary_input(large_queue, &telemetry.medium_to_large_steals);
        large_writer.set_secondary_input(medium_queue, &telemetry.large_to_medium_steals);
        large_writer.set_tertiary_input(small_queue, &telemetry.large_to_small_steals);

        const auto started_at = std::chrono::steady_clock::now();
        auto elapsed_since_start = [&]() {
            return std::chrono::duration<double>(std::chrono::steady_clock::now() - started_at).count();
        };

        std::cerr << "copy_target_start"
                  << " lanes=" << lanes
                  << " base_port=" << base_port
                  << " queue_depth=" << lane_queue_depth
                  << " pool_slots=" << receiver_pool_slots
                  << " integrated_folder_ready=1"
                  << std::endl;
        for (auto& lane : target_lanes) {
            lane->receiver->start();
        }
        std::cerr << "copy_target_receivers_started"
                  << " elapsed_s=" << elapsed_since_start()
                  << std::endl;
        small_writer.start();
        medium_writer.start();
        large_writer.start();

        std::vector<std::thread> spillway_drainers;
        spillway_drainers.reserve(2U);
        const auto drain_spillway = [&](TargetBufferSpillway& spillway,
                                        ShardedBufQueue& queue,
                                        std::size_t shard_count,
                                        std::string_view name) {
            try {
                while (!classifier_failed.load(std::memory_order_acquire)) {
                    std::vector<BufferHandle> batch = take_target_buffer_spill_batch(spillway, 1024U);
                    if (batch.empty()) {
                        std::lock_guard<std::mutex> lock(spillway.mutex);
                        if (spillway.input_done || spillway.stop || spillway.error) {
                            break;
                        }
                        continue;
                    }
                    for (const BufferHandle& spilled : batch) {
                        const DataBuffer& spilled_buffer = data_buffer(data_pool, spilled);
                        const std::uint64_t key = spilled_buffer.trailer.file_id != 0U
                                                      ? spilled_buffer.trailer.file_id
                                                      : hash64(spilled_buffer.trailer.rel_path.view());
                        if (!queue.push_wait(static_cast<std::size_t>(key % shard_count), spilled)) {
                            data_pool.release(spilled);
                            throw std::runtime_error(std::string("copy-target ") + std::string(name) +
                                                     " spillway drainer saw a closed writer queue");
                        }
                    }
                }
            } catch (...) {
                remember_classifier_error(std::current_exception());
            }
        };
        spillway_drainers.emplace_back(drain_spillway,
                                       std::ref(medium_spillway),
                                       std::ref(medium_queue),
                                       medium_threads,
                                       std::string_view("medium"));
        spillway_drainers.emplace_back(drain_spillway,
                                       std::ref(large_spillway),
                                       std::ref(large_queue),
                                       large_threads,
                                       std::string_view("large"));

        std::vector<std::thread> file_creators;
        file_creators.reserve(file_create_threads);
        if (precreate_target_files) {
            for (std::size_t creator_index = 0; creator_index < file_create_threads; ++creator_index) {
                file_creators.emplace_back([&, creator_index]() {
#if defined(__linux__)
                    const unsigned int hardware_cpus = std::thread::hardware_concurrency();
                    if (hardware_cpus != 0U) {
                        cpu_set_t set;
                        CPU_ZERO(&set);
                        CPU_SET(static_cast<int>((8U + (creator_index % 8U)) % hardware_cpus), &set);
                        (void)pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
                    }
#endif
                    TargetWriterBackend::Options create_options;
                    create_options.preserve_metadata = false;
                    create_options.fsync_on_finish = false;
                    create_options.ensure_parent_directories = false;
                    create_options.max_concurrent_file_transactions =
                        std::max<std::size_t>(1U, base_writer_config.max_concurrent_file_transactions);
                    auto create_backend = make_target_writer_backend(target_root, creator_index, create_options);

                    BufferHandle handle;
                    try {
                        while (!classifier_failed.load(std::memory_order_acquire) &&
                               file_create_queue.pop_wait(creator_index, handle)) {
                            bool transferred = false;
                            try {
                                create_buffer_files(*create_backend, handle);
                                transferred = route_created_buffer(handle);
                                classifier_buffers.fetch_add(1U, std::memory_order_relaxed);
                                if (!transferred) {
                                    data_pool.release(handle);
                                }
                            } catch (...) {
                                if (!transferred) {
                                    data_pool.release(handle);
                                }
                                throw;
                            }
                        }
                    } catch (...) {
                        precreated_files.fail(std::current_exception());
                        remember_classifier_error(std::current_exception());
                    }
                });
            }
        }

        std::vector<std::thread> classifiers;
        classifiers.reserve(lanes);
        for (std::size_t lane_index = 0; lane_index < lanes; ++lane_index) {
            classifiers.emplace_back([&, lane_index]() {
                pin_copy_target_classifier_thread(lane_index);
                TargetWriterBackend::Options folder_options;
                folder_options.preserve_metadata = false;
                folder_options.fsync_on_finish = false;
                folder_options.ensure_parent_directories = false;
                folder_options.max_concurrent_file_transactions =
                    std::max<std::size_t>(1U, base_writer_config.max_concurrent_file_transactions);
                auto folder_backend = make_target_writer_backend(target_root, lane_index, folder_options);
                BufferHandle handle;
                try {
                    while (!classifier_failed.load(std::memory_order_acquire) &&
                           target_lanes[lane_index]->queue.pop_wait(handle)) {
                        bool transferred = false;
                        try {
                            ensure_buffer_directories(*folder_backend, handle);
                            const DataBuffer& buffer = data_buffer(data_pool, handle);
                            const std::uint64_t key = is_packed_small_file_buffer(buffer)
                                                          ? packed_small_parent_locality_hash(
                                                                buffer,
                                                                buffer.trailer.folder_hash != 0U
                                                                    ? buffer.trailer.folder_hash
                                                                    : static_cast<std::uint64_t>(handle.index))
                                                          : (buffer.trailer.file_id != 0U
                                                                 ? buffer.trailer.file_id
                                                                 : hash64(buffer.trailer.rel_path.view()));
                            if (precreate_target_files) {
                                transferred = file_create_queue.push_wait(
                                    static_cast<std::size_t>(key % file_create_threads), handle);
                            } else {
                                transferred = route_created_buffer(handle);
                                if (transferred) {
                                    classifier_buffers.fetch_add(1U, std::memory_order_relaxed);
                                }
                            }
                            if (!transferred) {
                                data_pool.release(handle);
                            }
                        } catch (...) {
                            if (!transferred) {
                                data_pool.release(handle);
                            }
                            throw;
                        }
                    }
                } catch (...) {
                    remember_classifier_error(std::current_exception());
                }
            });
        }
        std::cerr << "copy_target_writers_started"
                  << " elapsed_s=" << elapsed_since_start()
                  << std::endl;

        std::thread telemetry_thread([&]() {
            try {
                using namespace std::chrono_literals;
                const auto rx_capacity = static_cast<std::int64_t>(lanes * lane_queue_depth);
                const auto create_capacity = static_cast<std::int64_t>(file_create_queue.capacity());
                const auto small_capacity = static_cast<std::int64_t>(small_queue.capacity());
                const auto medium_capacity = static_cast<std::int64_t>(medium_queue.capacity());
                const auto large_capacity = static_cast<std::int64_t>(large_queue.capacity());
                while (!telemetry_done.load(std::memory_order_acquire)) {
                    std::this_thread::sleep_for(200ms);
                    const auto mkdir_issued = telemetry.mkdir_requests_issued.load(std::memory_order_relaxed);
                    const auto mkdir_ack = telemetry.mkdir_completions_ack.load(std::memory_order_relaxed);
                    const auto mkdir_lag = mkdir_issued >= mkdir_ack ? mkdir_issued - mkdir_ack : 0U;
                    const double mkdir_avg_ms =
                        mkdir_ack == 0U
                            ? 0.0
                            : static_cast<double>(telemetry.mkdir_wait_ns.load(std::memory_order_relaxed)) /
                                  static_cast<double>(mkdir_ack) / 1'000'000.0;
                    const TargetWriterStats small_snapshot = small_writer.stats();
                    const TargetWriterStats medium_snapshot = medium_writer.stats();
                    const TargetWriterStats large_snapshot = large_writer.stats();
                    const std::uint64_t total_bytes_written =
                        small_snapshot.bytes_written + medium_snapshot.bytes_written + large_snapshot.bytes_written;
                    const double elapsed = elapsed_since_start();
                    const double sustained_gbit_s =
                        elapsed > 0.0 ? static_cast<double>(total_bytes_written) * 8.0 / elapsed / 1'000'000'000.0 : 0.0;
                    std::fprintf(stdout,
                                 "[T+%.1fs] RX_Q: [%lld/%lld] | MKDIR_Q: [%lld] (LAG: %llu issued=%llu ack=%llu avg_ms=%.3f) | CREATE_Q: [%lld/%lld] | SMALL_Q: [%lld/%lld routed=%llu] | MED_Q: [%lld/%lld] | LRG_Q: [%lld/%lld] | MED_SPILL: [%lld] | LRG_SPILL: [%lld] | STEAL_OPS: [LRG_TO_MED=%llu,LRG_TO_SMALL=%llu,MED_TO_SMALL=%llu,MED_TO_LRG=%llu] | SUSTAINED: %.2f Gbit/s | CLASSIFIED: %llu\n",
                                 elapsed,
                                 static_cast<long long>(telemetry.rx_queue_depth.load(std::memory_order_relaxed)),
                                 static_cast<long long>(rx_capacity),
                                 static_cast<long long>(telemetry.mkdir_queue_depth.load(std::memory_order_relaxed)),
                                 static_cast<unsigned long long>(mkdir_lag),
                                 static_cast<unsigned long long>(mkdir_issued),
                                 static_cast<unsigned long long>(mkdir_ack),
                                 mkdir_avg_ms,
                                 static_cast<long long>(telemetry.file_create_queue_depth.load(std::memory_order_relaxed)),
                                 static_cast<long long>(create_capacity),
                                 static_cast<long long>(telemetry.small_write_queue_depth.load(std::memory_order_relaxed)),
                                 static_cast<long long>(small_capacity),
                                 static_cast<unsigned long long>(small_buffers_routed.load(std::memory_order_relaxed)),
                                 static_cast<long long>(telemetry.medium_write_queue_depth.load(std::memory_order_relaxed)),
                                 static_cast<long long>(medium_capacity),
                                 static_cast<long long>(telemetry.large_write_queue_depth.load(std::memory_order_relaxed)),
                                 static_cast<long long>(large_capacity),
                                 static_cast<long long>(telemetry.medium_spillway_size.load(std::memory_order_relaxed)),
                                 static_cast<long long>(telemetry.large_spillway_size.load(std::memory_order_relaxed)),
                                 static_cast<unsigned long long>(telemetry.large_to_medium_steals.load(std::memory_order_relaxed)),
                                 static_cast<unsigned long long>(telemetry.large_to_small_steals.load(std::memory_order_relaxed)),
                                 static_cast<unsigned long long>(telemetry.medium_to_small_steals.load(std::memory_order_relaxed)),
                                 static_cast<unsigned long long>(telemetry.medium_to_large_steals.load(std::memory_order_relaxed)),
                                 sustained_gbit_s,
                                 static_cast<unsigned long long>(classifier_buffers.load(std::memory_order_relaxed)));
                    std::fflush(stdout);
                }
            } catch (const std::exception& error) {
                std::fprintf(stderr, "copy_target_telemetry_error error=%s\n", error.what());
                std::fflush(stderr);
            } catch (...) {
                std::fprintf(stderr, "copy_target_telemetry_error error=unknown\n");
                std::fflush(stderr);
            }
        });
        const auto stop_telemetry = [&]() {
            telemetry_done.store(true, std::memory_order_release);
            if (telemetry_thread.joinable()) {
                telemetry_thread.join();
            }
        };

        for (auto& lane : target_lanes) {
            lane->receiver->wait();
        }
        std::cerr << "copy_target_receivers_done"
                  << " elapsed_s=" << elapsed_since_start()
                  << std::endl;
        for (auto& classifier : classifiers) {
            classifier.join();
        }
        file_create_queue.close();
        for (auto& creator : file_creators) {
            if (creator.joinable()) {
                creator.join();
            }
        }
        mark_target_buffer_spillway_input_done(medium_spillway);
        mark_target_buffer_spillway_input_done(large_spillway);
        for (auto& drainer : spillway_drainers) {
            if (drainer.joinable()) {
                drainer.join();
            }
        }
        small_queue.close();
        medium_queue.close();
        large_queue.close();
        std::cerr << "copy_target_classifiers_done"
                  << " buffers=" << classifier_buffers.load(std::memory_order_relaxed)
                  << " elapsed_s=" << elapsed_since_start()
                  << std::endl;
        if (classifier_error) {
            std::cerr << "copy_target_classifier_error_shutdown"
                      << " elapsed_s=" << elapsed_since_start()
                      << std::endl;
            try {
                small_writer.stop();
            } catch (...) {
                std::cerr << "copy_target_stop_writer_error name=small error=" << current_exception_message()
                          << " elapsed_s=" << elapsed_since_start() << std::endl;
            }
            try {
                medium_writer.stop();
            } catch (...) {
                std::cerr << "copy_target_stop_writer_error name=medium error=" << current_exception_message()
                          << " elapsed_s=" << elapsed_since_start() << std::endl;
            }
            try {
                large_writer.stop();
            } catch (...) {
                std::cerr << "copy_target_stop_writer_error name=large error=" << current_exception_message()
                          << " elapsed_s=" << elapsed_since_start() << std::endl;
            }
            stop_telemetry();
            std::rethrow_exception(classifier_error);
        }
        std::exception_ptr writer_error;
        const auto remember_writer_error = [&](std::exception_ptr error) {
            if (!writer_error) {
                writer_error = error;
            }
            try {
                small_writer.stop();
            } catch (...) {
            }
            try {
                medium_writer.stop();
            } catch (...) {
            }
            try {
                large_writer.stop();
            } catch (...) {
            }
        };
        std::cerr << "copy_target_wait_writer name=small elapsed_s=" << elapsed_since_start() << std::endl;
        try {
            small_writer.wait();
            std::cerr << "copy_target_wait_writer_done name=small elapsed_s=" << elapsed_since_start() << std::endl;
        } catch (...) {
            std::cerr << "copy_target_wait_writer_error name=small error=" << current_exception_message()
                      << " elapsed_s=" << elapsed_since_start() << std::endl;
            remember_writer_error(std::current_exception());
        }
        std::cerr << "copy_target_wait_writer name=medium elapsed_s=" << elapsed_since_start() << std::endl;
        try {
            medium_writer.wait();
            std::cerr << "copy_target_wait_writer_done name=medium elapsed_s=" << elapsed_since_start() << std::endl;
        } catch (...) {
            std::cerr << "copy_target_wait_writer_error name=medium error=" << current_exception_message()
                      << " elapsed_s=" << elapsed_since_start() << std::endl;
            remember_writer_error(std::current_exception());
        }
        std::cerr << "copy_target_wait_writer name=large elapsed_s=" << elapsed_since_start() << std::endl;
        try {
            large_writer.wait();
            std::cerr << "copy_target_wait_writer_done name=large elapsed_s=" << elapsed_since_start() << std::endl;
        } catch (...) {
            std::cerr << "copy_target_wait_writer_error name=large error=" << current_exception_message()
                      << " elapsed_s=" << elapsed_since_start() << std::endl;
            remember_writer_error(std::current_exception());
        }
        stop_telemetry();
        if (writer_error) {
            std::rethrow_exception(writer_error);
        }
        std::cerr << "copy_target_writers_done"
                  << " elapsed_s=" << elapsed_since_start()
                  << std::endl;

        const TargetWriterStats small_snapshot = small_writer.stats();
        const TargetWriterStats medium_snapshot = medium_writer.stats();
        const TargetWriterStats large_snapshot = large_writer.stats();
        TransferReport report;
        for (const auto& lane : target_lanes) {
            const BufferTransportStats rx = lane->receiver->stats();
            report.chunks_sent += rx.buffers;
        }
        report.bytes_transferred =
            small_snapshot.bytes_written + medium_snapshot.bytes_written + large_snapshot.bytes_written;
        report.files_transferred =
            small_snapshot.files_written + medium_snapshot.files_written + large_snapshot.files_written;
        report.files_failed =
            small_snapshot.files_failed + medium_snapshot.files_failed + large_snapshot.files_failed;
        report.files_total = report.files_transferred + report.files_failed;
        report.elapsed_seconds =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - started_at).count();
        report.bytes_per_second =
            report.elapsed_seconds > 0.0 ? static_cast<double>(report.bytes_transferred) / report.elapsed_seconds
                                         : 0.0;
        std::string prefix_pipeline =
            "[BufferReceiver-1 x" + std::to_string(lanes) +
            "]->(RecvDataBufQueue-" + std::to_string(lane_queue_depth) +
            " x" + std::to_string(lanes) + ")->[ReadyClassifier-NFS-1 x" +
            std::to_string(lanes) + "]";
        if (precreate_target_files) {
            prefix_pipeline +=
                "->(TargetFileCreateQueue-" + std::to_string(file_create_queue_depth_per_shard) +
                " x" + std::to_string(file_create_threads) + ")->[TargetFileCreation-NFS-" +
                std::to_string(file_create_threads) + "]";
        }
        report.pipeline_description =
            prefix_pipeline + "->(SmallReadyFileQueue-" +
            std::to_string(small_queue_depth_per_shard) + " x" + std::to_string(small_threads) +
            " parent-hash)->[DataWriter-NFS/reactors=" +
            std::to_string(base_writer_config.reactor_count) + " window=" +
            std::to_string(base_writer_config.max_concurrent_file_transactions) +
            " batch=" + std::to_string(small_writer_config.async_window) +
            "]+(MediumReadyBufQueue-" + std::to_string(target_bulk_queue_depth_per_shard) + " x" +
            std::to_string(medium_threads) + ")->[DataWriter-NFS-" +
            std::to_string(medium_threads) + " steal=SMALL,LRG]+(LargeReadyBufQueue-" +
            std::to_string(target_bulk_queue_depth_per_shard) + " x" + std::to_string(large_threads) +
            ")->[DataWriter-NFS-" + std::to_string(large_threads) +
            " batch=" + std::to_string(large_writer_config.async_window) +
            " steal=MED,SMALL governor=on]";
        (void)folders_created;
        return report;
    }

    struct CopyTargetLane {
        RawBufferPool pool;
        BufferPoolRegistry registry;
        BufQueue receiver_queue;
        BufQueue writer_queue;
        std::unique_ptr<BufferReceiverJob> receiver;
        std::unique_ptr<TargetDataFolderGateJob> folder_gate;
        std::unique_ptr<TargetDataWriterJob> writer;

        CopyTargetLane(std::size_t pool_slots, std::size_t queue_depth)
            : pool(make_data_buffer_pool(pool_slots)),
              receiver_queue(queue_depth),
              writer_queue(queue_depth) {
            registry.register_pool(pool);
        }
    };

    std::vector<std::unique_ptr<CopyTargetLane>> target_lanes;
    target_lanes.reserve(lanes);
    for (std::size_t lane = 0; lane < lanes; ++lane) {
        auto target_lane = std::make_unique<CopyTargetLane>(data_buffer_slots_per_lane, lane_queue_depth);
        TargetDataWriterConfig writer_config = load_target_data_writer_config(config_store_);
        writer_config.worker_count = 1U;
        writer_config.target_root = target_root;
        writer_config.endpoint_index_offset = lane;
        writer_config.verify_hash = verify_hash;
        writer_config.preserve_metadata = preserve_metadata;
        writer_config.fsync_on_finish = target_fsync;
        writer_config.ensure_parent_directories = ensure_target_directories;
        writer_config.reactor_count = writer_reactors;
        writer_config.reactors_per_ip = std::max<std::size_t>(1U, reactors_per_ip);
        writer_config.direct_reactor_submit = false;
        writer_config.direct_reactor_writes = false;
        if (writer_async_window != 0U) {
            writer_config.async_window = writer_async_window;
        }
        if (writer_file_window != 0U) {
            writer_config.max_concurrent_file_transactions = writer_file_window;
        }
        target_lane->receiver = std::make_unique<BufferReceiverJob>(
            1U,
            target_lane->pool,
            target_lane->receiver_queue,
            BufferTransportEndpoint::tcp(bind_host.empty() ? std::string("0.0.0.0") : bind_host,
                                         static_cast<std::uint16_t>(base_port + lane)));
        BufQueue* writer_input = &target_lane->receiver_queue;
        if (ensure_target_directories) {
            TargetDataWriterConfig folder_gate_config = writer_config;
            folder_gate_config.worker_count = 1U;
            folder_gate_config.preserve_metadata = false;
            folder_gate_config.fsync_on_finish = false;
            folder_gate_config.ensure_parent_directories = false;
            target_lane->folder_gate = std::make_unique<TargetDataFolderGateJob>(folder_gate_config,
                                                                                 target_lane->pool,
                                                                                 target_lane->receiver_queue,
                                                                                 target_lane->writer_queue);
            writer_config.ensure_parent_directories = false;
            writer_input = &target_lane->writer_queue;
        }
        target_lane->writer = std::make_unique<TargetDataWriterJob>(writer_config,
                                                                    target_lane->pool,
                                                                    *writer_input);
        target_lanes.push_back(std::move(target_lane));
    }

    const auto started_at = std::chrono::steady_clock::now();
    auto elapsed_since_start = [&]() {
        return std::chrono::duration<double>(std::chrono::steady_clock::now() - started_at).count();
    };

    std::cerr << "copy_target_start"
              << " lanes=" << lanes
              << " base_port=" << base_port
              << " queue_depth=" << lane_queue_depth
              << " pool_slots_per_lane=" << data_buffer_slots_per_lane
              << " shared_nothing=" << (shared_nothing ? 1 : 0)
              << std::endl;
    for (auto& lane : target_lanes) {
        lane->receiver->start();
    }
    std::cerr << "copy_target_receivers_started"
              << " elapsed_s=" << elapsed_since_start()
              << std::endl;
    for (auto& lane : target_lanes) {
        lane->writer->start();
    }
    for (auto& lane : target_lanes) {
        if (lane->folder_gate != nullptr) {
            lane->folder_gate->start();
        }
    }
    std::cerr << "copy_target_writers_started"
              << " elapsed_s=" << elapsed_since_start()
              << std::endl;
    for (auto& lane : target_lanes) {
        lane->receiver->wait();
    }
    std::cerr << "copy_target_receivers_done"
              << " elapsed_s=" << elapsed_since_start()
              << std::endl;
    for (auto& lane : target_lanes) {
        if (lane->folder_gate != nullptr) {
            lane->folder_gate->wait();
        }
    }
    std::cerr << "copy_target_folder_gates_done"
              << " elapsed_s=" << elapsed_since_start()
              << std::endl;
    for (auto& lane : target_lanes) {
        lane->writer->wait();
    }
    std::cerr << "copy_target_writers_done"
              << " elapsed_s=" << elapsed_since_start()
              << std::endl;

    TransferReport report;
    for (const auto& lane : target_lanes) {
        const BufferTransportStats rx = lane->receiver->stats();
        const TargetWriterStats writer = lane->writer->stats();
        report.chunks_sent += rx.buffers;
        report.bytes_transferred += writer.bytes_written;
        report.files_transferred += writer.files_written;
        report.files_failed += writer.files_failed;
    }
    report.files_total = report.files_transferred + report.files_failed;
    report.elapsed_seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started_at).count();
    report.bytes_per_second =
        report.elapsed_seconds > 0.0 ? static_cast<double>(report.bytes_transferred) / report.elapsed_seconds : 0.0;
    report.pipeline_description =
        ensure_target_directories
            ? std::string(shared_nothing ? "[BufferReceiver-1 x" : "[BufferReceiver-1 x") + std::to_string(lanes) +
                  "]->(RecvDataBufQueue-" + std::to_string(lane_queue_depth) +
                  " x" + std::to_string(lanes) + ")->[FolderCreation-NFS-1 x" +
                  std::to_string(lanes) + "]->(ReadyDataBufQueue-" +
                  std::to_string(lane_queue_depth) + " x" + std::to_string(lanes) +
                  ")->[DataWriter-NFS-1 x" + std::to_string(lanes) + "]"
            : "[BufferReceiver-1 x" + std::to_string(lanes) +
                  "]->(DataBufQueue-" + std::to_string(lane_queue_depth) +
                  " x" + std::to_string(lanes) + ")->[DataWriter-NFS-1 x" +
                  std::to_string(lanes) + "]";
    return report;
}

TransferReport TransferEngine::transfer_directory(const SenderRuntimeConfig& runtime) const {
    auto files = scan_directory_with_config(runtime.source_root, runtime.recursive);
    auto directories = scan_directories_for_transfer(runtime.source_root, runtime.recursive);
    std::sort(files.begin(), files.end(), [](const FileSpec& lhs, const FileSpec& rhs) {
        return lhs.rel_path < rhs.rel_path;
    });
    std::sort(directories.begin(), directories.end(), [](const FileSpec& lhs, const FileSpec& rhs) {
        if (path_depth(lhs.rel_path) != path_depth(rhs.rel_path)) {
            return path_depth(lhs.rel_path) < path_depth(rhs.rel_path);
        }
        return lhs.rel_path < rhs.rel_path;
    });

    TransferReport report;
    report.mode = Mode::transfer;
    report.files_total = files.size();
    report.folders_total = 0;
    for (const auto& file : files) {
        initialize_folder_report(report, file);
        report.bytes_planned += file.declared_size;
    }
    report.folders_total = report.folders.size();

    ScopedFd priority_fd = connect_tcp(runtime.remote_host, runtime.priority_port);
    ScopedFd data_fd = connect_tcp(runtime.remote_host, runtime.data_port);
    send_session_start(priority_fd.get(), config_);
    for (const auto& directory : directories) {
        send_directory_record(priority_fd.get(), make_directory_record_message(directory));
    }

    const bool remote_source = is_nfs_url(runtime.source_root.string());
    NfsDataReaderConfig data_reader_config = load_nfs_data_reader_config(config_store_);
    data_reader_config.small_file_threshold = config_.small_file_threshold;
    data_reader_config.large_chunk_bytes = config_.large_chunk_bytes;
    data_reader_config.source_root = runtime.source_root.string();
    data_reader_config.outstanding_requests =
        std::min<std::size_t>(std::max<std::size_t>(1, config_.large_pool_slots),
                              std::max<std::size_t>(1, data_reader_config.outstanding_requests));
    NfsDataReader data_reader(data_reader_config);
    const bool enable_cache = !runtime.cache_root.empty();
    DataCacherConfig cache_config = load_data_cacher_config(config_store_);
    cache_config.role = EndpointRole::sender;
    if (!runtime.cache_root.empty()) {
        cache_config.cache_path = runtime.cache_root.string();
    }
    DataCacher cacher(cache_config);
    DataSlotPool sender_slots(config_.small_pool_slots, config_.large_pool_slots);
    DataSlotPool cache_slots(config_.small_pool_slots, config_.large_pool_slots);
    NfsDataReader stream_data_reader(data_reader_config);
    bool paused = false;
    std::deque<FileAckMessage> pending_acks;
    std::vector<PendingTransferFile> decision_files;
    decision_files.reserve(files.size());
    std::vector<PendingTransferFile> pending_files;
    pending_files.reserve(files.size());

    for (const auto& file : files) {
        const FileRecordMessage record = make_file_record_message(file);
        send_file_record(priority_fd.get(), record);
        decision_files.push_back(PendingTransferFile{file, record});
    }

    for (const auto& pending : decision_files) {
        PriorityMessageType decision_type = PriorityMessageType::session_end;
        std::string decision_payload;
        if (!read_priority_payload(priority_fd.get(), decision_type, decision_payload) ||
            decision_type != PriorityMessageType::file_decision) {
            throw std::runtime_error("expected file decision from receiver");
        }

        const FileDecisionMessage decision = decode_file_decision(decision_payload);
        if (decision.file_id != pending.record.file_id) {
            throw std::runtime_error("receiver decision file_id mismatch");
        }

        FileOutcome outcome;
        outcome.rel_path = pending.file.rel_path;
        outcome.size = pending.file.declared_size;
        outcome.sender_state = FileState::checking;
        outcome.receiver_state = FileState::pending;

        FolderRecord& folder = report.folders[parent_path(pending.file.rel_path)];
        if (decision.skip) {
            outcome.diff = DiffKind::skip;
            outcome.sender_state = FileState::done;
            outcome.receiver_state = FileState::done;
            outcome.hash_verified = true;
            ++report.files_skipped;
            ++folder.files_skipped;
            ++folder.files_completed;
            ++folder.files_received;
            report.files[pending.file.rel_path] = outcome;
            continue;
        }
        pending_files.push_back(pending);
    }

    PreparedTransferQueue prepared_queue;
    prepared_queue.max_entries = std::max<std::size_t>(1, data_reader.config().large_file_parallelism);
    prepared_queue.queue.reset(prepared_queue.max_entries);
    std::thread prepare_thread(produce_prepared_transfers,
                               std::ref(prepared_queue),
                               std::cref(pending_files),
                               remote_source,
                               std::cref(runtime.source_root),
                               std::ref(data_reader),
                               std::ref(cacher),
                               std::ref(cache_slots),
                               std::cref(config_),
                               runtime.cache_file_threshold_bytes,
                               enable_cache);

    std::unordered_map<std::uint64_t, AwaitingAckTransfer> awaiting_acks;
    awaiting_acks.reserve(pending_files.size());

    try {
        PreparedTransfer prepared;
        std::optional<PreparedTransfer> carried_prepared;
        while (carried_prepared.has_value() || pop_prepared_transfer(prepared_queue, prepared)) {
            if (carried_prepared.has_value()) {
                prepared = std::move(*carried_prepared);
                carried_prepared.reset();
            }

            if (can_pack_small_file(prepared, config_)) {
                std::vector<PreparedTransfer> batch;
                batch.push_back(std::move(prepared));
                std::size_t packed_bytes = kPackedSmallFileCountBytes + packed_small_file_entry_bytes(batch.back());

                PreparedTransfer next;
                while (try_pop_prepared_transfer(prepared_queue, next)) {
                    if (!can_pack_small_file(next, config_)) {
                        carried_prepared = std::move(next);
                        break;
                    }
                    const std::size_t next_bytes = packed_small_file_entry_bytes(next);
                    if (packed_bytes + next_bytes > config_.large_chunk_bytes) {
                        carried_prepared = std::move(next);
                        break;
                    }
                    packed_bytes += next_bytes;
                    batch.push_back(std::move(next));
                }

                const std::vector<std::uint64_t> hashes =
                    send_packed_small_file_batch(data_fd.get(),
                                                 sender_slots,
                                                 batch,
                                                 config_,
                                                 priority_fd.get(),
                                                 paused,
                                                 pending_acks,
                                                 report);
                for (std::size_t index = 0; index < batch.size(); ++index) {
                    const std::uint64_t file_id = batch[index].record.file_id;
                    awaiting_acks.emplace(file_id,
                                          AwaitingAckTransfer{std::move(batch[index]), hashes[index], 1U});
                }
                consume_sender_acks(pending_acks, awaiting_acks, report);
                continue;
            }

            if (prepared.cached) {
                for (const std::uint64_t entry_id : prepared.cached_entry_ids) {
                    drain_sender_priority_events(priority_fd.get(), 0, paused, pending_acks);
                    wait_for_sender_resume(priority_fd.get(), paused, pending_acks);

                    const DataSlotHandle handle = cacher.take_slot(entry_id, sender_slots);
                    send_data_slot(data_fd.get(), sender_slots, handle);
                    sender_slots.release(handle);
                    ++report.chunks_sent;
                }
            } else if (prepared.content_loaded) {
                send_content_file_slots(data_fd.get(),
                                        sender_slots,
                                        prepared.file,
                                        config_,
                                        priority_fd.get(),
                                        paused,
                                        pending_acks,
                                        report);
            } else if (remote_source) {
                prepared.data_hash = send_remote_nfs_file_slots(data_fd.get(),
                                                                sender_slots,
                                                                stream_data_reader,
                                                                prepared.file,
                                                                config_,
                                                                priority_fd.get(),
                                                                paused,
                                                                pending_acks,
                                                                report);
            } else {
                prepared.data_hash = send_local_file_slots(data_fd.get(),
                                                           sender_slots,
                                                           prepared.source_path,
                                                           prepared.file,
                                                           config_,
                                                           priority_fd.get(),
                                                           paused,
                                                           pending_acks,
                                                           report);
            }

            const std::uint64_t file_id = prepared.record.file_id;
            const std::uint64_t data_hash = prepared.data_hash;
            const std::size_t chunk_count = prepared.chunk_count;
            awaiting_acks.emplace(file_id,
                                  AwaitingAckTransfer{std::move(prepared), data_hash, chunk_count});
            consume_sender_acks(pending_acks, awaiting_acks, report);
        }

        while (!awaiting_acks.empty()) {
            const FileAckMessage ack = wait_for_sender_ack(priority_fd.get(), paused, pending_acks);
            record_sender_ack(ack, awaiting_acks, report);
            consume_sender_acks(pending_acks, awaiting_acks, report);
        }
    } catch (...) {
        {
            std::lock_guard<std::mutex> lock(prepared_queue.mutex);
            prepared_queue.stop = true;
        }
        prepared_queue.cv_not_full.notify_all();
        prepared_queue.cv_not_empty.notify_all();
        prepare_thread.join();
        throw;
    }

    prepare_thread.join();

    send_session_end(priority_fd.get());
    ::shutdown(data_fd.get(), SHUT_WR);
    finalize_folder_report(report);
    return report;
}

void TransferEngine::run_receiver(const ReceiverRuntimeConfig& runtime) const {
    if (!is_nfs_url(runtime.target_root.string())) {
        std::filesystem::create_directories(runtime.target_root);
    }

    ScopedFd priority_listener = listen_tcp(runtime.bind_host, runtime.priority_port);
    ScopedFd data_listener = listen_tcp(runtime.bind_host, runtime.data_port);
    ScopedFd priority_fd = accept_tcp(priority_listener.get());
    ScopedFd data_fd = accept_tcp(data_listener.get());

    ReceiverSharedState state;
    state.runtime = runtime;
    state.skip_verify = config_.skip_verify;
    state.small_pool_slots = config_.small_pool_slots;
    state.large_pool_slots = config_.large_pool_slots;
    state.priority_fd = priority_fd.get();
    state.data_fd = data_fd.get();

    std::exception_ptr priority_error;
    std::exception_ptr data_error;

    std::thread priority_thread([&] {
        try {
            receiver_priority_loop(state);
        } catch (...) {
            priority_error = std::current_exception();
            shutdown_receiver_sockets(state);
        }
    });

    std::thread data_thread([&] {
        try {
            receiver_data_loop(state, data_fd.get());
        } catch (...) {
            data_error = std::current_exception();
            shutdown_receiver_sockets(state);
        }
    });

    priority_thread.join();
    data_thread.join();

    if (priority_error != nullptr) {
        std::rethrow_exception(priority_error);
    }
    if (data_error != nullptr) {
        std::rethrow_exception(data_error);
    }

    if (!state.directory_specs.empty()) {
        std::sort(state.directory_specs.begin(), state.directory_specs.end(), [](const FileSpec& lhs, const FileSpec& rhs) {
            if (path_depth(lhs.rel_path) != path_depth(rhs.rel_path)) {
                return path_depth(lhs.rel_path) > path_depth(rhs.rel_path);
            }
            return lhs.rel_path > rhs.rel_path;
        });
        auto target_writer = make_target_writer_backend(runtime.target_root.string());
        for (const auto& directory : state.directory_specs) {
            target_writer->apply_directory_metadata(directory);
        }
    }
}

ScanIndex TransferEngine::load_scan_csv(const std::filesystem::path& input_path) {
    return ScanIndex::from_csv(read_text_file(input_path));
}

void TransferEngine::write_scan_csv(const ScanIndex& index, const std::filesystem::path& output_path) {
    write_text_file(output_path, index.to_csv());
}

void TransferEngine::write_diff_csv(const TransferReport& report, const std::filesystem::path& output_path) {
    write_text_file(output_path, report.diff_csv);
}

}  // namespace hypersync
