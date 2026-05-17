#include "hypersync.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
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
#include <thread>
#include <vector>

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
            << " small=" << phase.small_file_count
            << " large=" << phase.large_file_count
            << " small_ratio=" << small_ratio
            << " logical_size_bytes=" << phase.logical_size_bytes
            << " avg_filename_len=" << average_filename_length
            << " avg_depth=" << average_depth
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
        out << '\n';
    }
}

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
    std::uint64_t filename_length_sum = 0;
    std::uint64_t depth_sum = 0;
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
    for (std::size_t index = 0; index < target.size_file_counts.size(); ++index) {
        target.size_file_counts[index] += source.size_file_counts[index];
        target.size_logical_bytes[index] += source.size_logical_bytes[index];
    }
    for (std::size_t index = 0; index < target.folder_fanout_counts.size(); ++index) {
        target.folder_fanout_counts[index] += source.folder_fanout_counts[index];
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
        phase.filename_length_sum = accumulator.filename_length_sum;
        phase.depth_sum = accumulator.depth_sum;
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
                                            std::uint64_t small_threshold) {
    NfsProfileWorkQueue queue;
    queue.folders.push_back(hypersync::FileSpec {});
    std::vector<FixedPhaseAccumulator> accumulators(std::max<std::size_t>(1U, phase_count));
    std::vector<std::mutex> accumulator_mutexes(accumulators.size());
    std::atomic<std::uint64_t> files_reserved {0};
    std::atomic<std::uint64_t> files_recorded {0};
    std::atomic<std::uint64_t> folders_observed {1};
    std::atomic<std::uint64_t> failed_folders {0};
    const std::uint64_t records_per_phase =
        std::max<std::uint64_t>(1U, (max_records + accumulators.size() - 1U) / accumulators.size());
    const auto started = std::chrono::steady_clock::now();

    const auto should_stop = [&]() {
        return files_reserved.load(std::memory_order_acquire) >= max_records;
    };

    std::vector<std::thread> workers;
    workers.reserve(std::max<std::size_t>(1U, meta_reader_threads));
    for (std::size_t worker_index = 0; worker_index < std::max<std::size_t>(1U, meta_reader_threads); ++worker_index) {
        (void)worker_index;
        workers.emplace_back([&]() {
            try {
                auto backend = hypersync::make_nfs_backend(source_root.string(),
                                                           hypersync::kNfsEndpointAny,
                                                           readdirplus_page_bytes);
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
                        }
                        if (accepted_file_count != 0U) {
                            const std::size_t folder_phase_index = std::min<std::size_t>(
                                accumulators.size() - 1U,
                                static_cast<std::size_t>(start_index / records_per_phase));
                            local[folder_phase_index].folder_count += 1U + batch.directories.size();
                            folders_observed.fetch_add(1U + batch.directories.size(),
                                                       std::memory_order_relaxed);
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
        << "  hypersync [--config <config.yaml>] send|sync|copy --source <dir|nfs-url> [--host <host>] [--priority-port <port>] [--data-port <port>] [--cache-path <dir>] [--cache-threshold <bytes>] [--skip-verify]\n"
        << "  hypersync [--config <config.yaml>] scan --source <dir|nfs-url> --output <scan.csv|txt|parquet> [--scan-side S|T] [--output-format text|csv|parquet] [--records all|files|folders] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--record-buffer-slots <n>] [--pipeline-autoscale|--no-pipeline-autoscale] [--autoscale-profile <name>] [--autoscale-settings <path>] [--autoscale-interval-ms <n>] [--max-duration-seconds <n>] [--stats-interval-seconds <n>] [--status-socket <path>]\n"
        << "  hypersync [--config <config.yaml>] diff (--source <dir|nfs-url> --target <dir|nfs-url> | --source-scan <scan.csv> --target-scan <scan.csv>) [--compare size|time|content] [--summary-only] [--output <diff.csv>] [--non-recursive] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--checker-threads <n>] [--checker-request-queue-depth <n>] [--checker-batch-queue-depth <n>] [--max-duration-seconds <n>] [--stats-interval-seconds <n>]\n"
        << "  hypersync [--config <config.yaml>] diff-target --target <dir|nfs-url> [--listen-host <host>] --port <port> [--compare size|time|content] [--non-recursive] [--target-threads <n>] [--metadata-async-depth <n>] [--pipeline-autoscale] [--autoscale-profile <name>] [--autoscale-settings <path>] [--autoscale-interval-ms <n>] [--stats-interval-seconds <n>]\n"
        << "  hypersync [--config <config.yaml>] diff-source --source <dir|nfs-url> --target-host <host> --port <port> --folder-report <report.csv> [--compare size|time|content] [--non-recursive] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--pipeline-autoscale] [--autoscale-profile <name>] [--autoscale-settings <path>] [--autoscale-interval-ms <n>] [--max-duration-seconds <n>] [--stats-interval-seconds <n>]\n"
        << "  hypersync [--config <config.yaml>] dry-run --source <dir|nfs-url> [--source-scan <scan.csv>] [--target-scan <scan.csv>] [--output <diff.csv>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-meta --source <dir|nfs-url> [--non-recursive] [--discard-after-checker|--keep-after-checker|--metadata-stats-discarder] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--metadata-output <path>] [--metadata-output-format text|csv|parquet] [--metadata-records all|files|folders] [--metadata-output-partitions <n>] [--metadata-output-partition-mode single|processes] [--record-buffer-slots <n>] [--pipeline-autoscale|--no-pipeline-autoscale] [--autoscale-profile <name>] [--autoscale-settings <path>] [--autoscale-interval-ms <n>] [--max-duration-seconds <n>] [--stats-interval-seconds <n>] [--status-socket <path>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-open --source <dir|nfs-url> [--non-recursive] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--open-threads <n>] [--max-files-queued <n>] [--max-duration-seconds <n>] [--stats-interval-seconds <n>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-data --source <dir|nfs-url> [--non-recursive] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--readdirplus-page-bytes <n>] [--data-reader-threads <n>] [--data-outstanding-requests <n>] [--small-file-async-window <n>] [--split-small-large] [--dual-scan-small-large] [--background-recon-scan] [--bucket-priority] [--morph-large-readers-to-small] [--small-file-threshold-bytes <n>] [--recon-meta-reader-threads <n>] [--recon-metadata-async-depth <n>] [--recon-page-sleep-us <n>] [--small-meta-reader-threads <n>] [--large-meta-reader-threads <n>] [--small-data-reader-threads <n>] [--large-data-reader-threads <n>] [--large-data-outstanding-requests <n>] [--pipeline-autoscale] [--large-reader-autoscale] [--large-reader-initial-threads <n>] [--autoscale-interval-ms <n>] [--autoscale-profile <name>] [--autoscale-settings <path>] [--max-file-size-bytes <n>] [--pack-small-files] [--max-files-queued <n>] [--small-max-files-queued <n>] [--large-max-files-queued <n>] [--data-buffer-slots <n>] [--data-queue-depth <n>] [--data-copy-mode copy|no-copy] [--max-duration-seconds <n>] [--stats-interval-seconds <n>] [--status-socket <path>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-data-hash --source <dir|nfs-url> [--hash md5|sha256|xxh64|xxh3_64|xxh3_128] [--non-recursive] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--data-reader-threads <n>] [--data-outstanding-requests <n>] [--small-file-async-window <n>] [--pack-small-files] [--hash-threads <n>] [--hash-work-factor <n>] [--max-files-queued <n>] [--data-buffer-slots <n>] [--data-queue-depth <n>] [--max-duration-seconds <n>] [--stats-interval-seconds <n>] [--status-socket <path>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-synthetic-profile [--file-count <n>] [--block-file-count <n>] [--small-ratio-shift-threshold <n>] [--seed <n>] [--output <profile.txt>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-nfs-profile --source <nfs-url> [--max-records <n>] [--phase-count <n>] [--non-recursive] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--readdirplus-page-bytes <n>] [--small-file-threshold-bytes <n>] [--output <profile.txt>]\n"
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

        if (command == "benchmark-nfs-profile") {
            std::filesystem::path source_root;
            bool recursive = true;
            std::uint64_t max_records = 100'000'000ULL;
            std::size_t phase_count = 10;
            std::size_t meta_reader_threads = 96;
            std::size_t metadata_async_depth = 256;
            std::size_t readdirplus_page_bytes = 256U * 1024U;
            std::uint64_t small_threshold = hypersync::kSmallFileThreshold;
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
                                                                       small_threshold);
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
                    discard_after_checker = false;
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
