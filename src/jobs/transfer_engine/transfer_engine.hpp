#ifndef HYPERSYNC_JOBS_TRANSFER_ENGINE_HPP
#define HYPERSYNC_JOBS_TRANSFER_ENGINE_HPP

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "common/config.hpp"
#include "common/scan_index.hpp"
#include "common/types.hpp"

namespace hypersync {

struct SenderRuntimeConfig {
    std::filesystem::path source_root;
    std::string remote_host;
    std::uint16_t priority_port;
    std::uint16_t data_port;
    bool recursive;
    std::filesystem::path cache_root;
    std::uint64_t cache_file_threshold_bytes;

    SenderRuntimeConfig();
    SenderRuntimeConfig(std::filesystem::path source_root,
                        std::string remote_host,
                        std::uint16_t priority_port,
                        std::uint16_t data_port,
                        bool recursive,
                        std::filesystem::path cache_root,
                        std::uint64_t cache_file_threshold_bytes);
};

struct ReceiverRuntimeConfig {
    std::filesystem::path target_root;
    std::string bind_host;
    std::uint16_t priority_port;
    std::uint16_t data_port;
    std::uint64_t backpressure_window_bytes;
    std::uint32_t backpressure_pause_ms;

    ReceiverRuntimeConfig();
    ReceiverRuntimeConfig(std::filesystem::path target_root,
                          std::string bind_host,
                          std::uint16_t priority_port,
                          std::uint16_t data_port,
                          std::uint64_t backpressure_window_bytes,
                          std::uint32_t backpressure_pause_ms);
};

struct MetadataBenchmarkReport {
    std::size_t files_seen = 0;
    std::size_t checker_emitted = 0;
    std::size_t checker_discarded = 0;
    std::size_t folders_found = 0;
    std::uint64_t logical_size_bytes = 0;
    double records_per_second = 0.0;
    std::size_t meta_reader_threads = 0;
    std::size_t metadata_async_depth = 0;
    std::size_t metadata_files_written = 0;
    std::size_t metadata_folders_written = 0;
    std::size_t metadata_output_partitions = 1;
    std::size_t record_buffer_slots = 0;
    std::size_t metadata_queue_shards = 1;
    std::size_t metadata_queue_capacity = 0;
    std::size_t metadata_queue_high_watermark = 0;
    bool metadata_queue_full = false;
    std::uint32_t stats_interval_seconds = 0;
    std::string scan_run_id;
    bool pipeline_autoscale = false;
    std::uint64_t autoscale_interval_ms = 0;
    std::string autoscale_profile;
    std::filesystem::path autoscale_settings_path;
    std::size_t learned_meta_reader_threads = 0;
    bool meta_reader_async = false;
    double elapsed_seconds = 0.0;
};

struct DataReadBenchmarkReport {
    std::size_t files_found = 0;
    std::size_t folders_found = 0;
    std::size_t files_read = 0;
    std::size_t files_failed = 0;
    std::uint64_t logical_size_bytes = 0;
    std::uint64_t bytes_read = 0;
    std::size_t small_files_found = 0;
    std::size_t large_files_found = 0;
    std::size_t small_files_read = 0;
    std::size_t large_files_read = 0;
    std::uint64_t small_bytes_read = 0;
    std::uint64_t large_bytes_read = 0;
    std::uint64_t small_logical_size_bytes = 0;
    std::uint64_t large_logical_size_bytes = 0;
    std::size_t recon_files_found = 0;
    std::size_t recon_folders_found = 0;
    std::size_t recon_small_files_found = 0;
    std::size_t recon_large_files_found = 0;
    std::uint64_t recon_logical_size_bytes = 0;
    std::uint64_t recon_small_logical_size_bytes = 0;
    std::uint64_t recon_large_logical_size_bytes = 0;
    bool recon_completed = false;
    double bytes_per_second = 0.0;
    double gigabits_per_second = 0.0;
    double files_per_second = 0.0;
    double small_files_per_second = 0.0;
    double large_files_per_second = 0.0;
    double small_gigabits_per_second = 0.0;
    double large_gigabits_per_second = 0.0;
    double elapsed_seconds = 0.0;
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
    std::uint64_t autoscale_interval_ms = 0;
    std::string autoscale_profile;
    std::filesystem::path autoscale_settings_path;
    std::uint64_t min_file_size_bytes = 0;
    std::uint64_t max_file_size_bytes = 0;
    std::size_t max_files_queued = 0;
    std::size_t small_max_files_queued = 0;
    std::size_t large_max_files_queued = 0;
    std::size_t data_buffer_slots = 0;
    std::size_t data_queue_depth = 0;
    std::string data_copy_mode;
    bool pack_small_files = false;
    bool meta_reader_async = false;
    bool data_reader_async = false;
    std::uint64_t async_read_queued = 0;
    std::uint64_t async_read_completed = 0;
    std::uint64_t async_read_short = 0;
    std::uint64_t async_read_failed = 0;
    std::uint64_t async_read_zero = 0;
    std::uint64_t async_read_bytes_requested = 0;
    std::uint64_t async_read_bytes_completed = 0;
    double async_read_avg_latency_ms = 0.0;
    double async_read_max_latency_ms = 0.0;
    std::uint64_t async_open_completed = 0;
    std::uint64_t async_open_failed = 0;
    double async_open_avg_latency_ms = 0.0;
    double async_open_max_latency_ms = 0.0;
    std::uint64_t async_close_completed = 0;
    std::uint64_t async_close_failed = 0;
    double async_close_avg_latency_ms = 0.0;
    double async_close_max_latency_ms = 0.0;
    std::size_t files_written = 0;
    std::size_t write_failed = 0;
    std::size_t folders_written = 0;
    double folders_per_second = 0.0;
    std::uint64_t bytes_written = 0;
    std::size_t data_writer_threads = 0;
    std::size_t data_writer_async_window = 1;
    std::size_t data_writer_file_window = 64;
    std::size_t data_writer_reactors = 0;
    std::size_t data_queue_shards = 0;
    std::size_t data_queue_capacity = 0;
    std::size_t data_queue_high_watermark = 0;
    std::string target_root;
};

struct SplitBucketPriorityInput {
    std::uint64_t small_total = 0;
    std::uint64_t large_total = 0;
    std::uint64_t large_total_bytes = 0;
    std::uint64_t small_done = 0;
    std::uint64_t large_done = 0;
    std::uint64_t large_done_bytes = 0;
    double small_files_per_second = 0.0;
    double large_files_per_second = 0.0;
    double large_bytes_per_second = 0.0;
    std::size_t current_small_workers = 1;
    std::size_t current_large_workers = 1;
    std::size_t max_small_workers = 1;
    std::size_t max_large_workers = 1;
};

struct SplitBucketPriorityDecision {
    std::size_t small_workers = 1;
    std::size_t large_workers = 1;
    std::uint32_t large_reader_small_priority_percent = 0;
    double small_eta_seconds = 0.0;
    double large_eta_seconds = 0.0;
};

[[nodiscard]] SplitBucketPriorityDecision choose_split_bucket_priority_workers(
    const SplitBucketPriorityInput& input) noexcept;

struct BucketPathOverloadInput {
    double small_eta_seconds = 0.0;
    double large_eta_seconds = 0.0;
    std::size_t queued_small_files = 0;
    std::size_t small_low_watermark_files = 0;
    std::size_t small_high_watermark_files = 0;
    std::uint64_t small_low_watermark_intervals = 0;
    double small_scanner_sleep_ratio = 0.0;
    std::size_t queued_large_files = 0;
    std::size_t large_queue_capacity_files = 0;
    double total_gigabits_per_second = 0.0;
    double large_gigabits_per_second = 0.0;
    double line_rate_gigabits_per_second = 196.0;
    double large_overload_floor_gigabits_per_second = 130.0;
};

struct BucketPathOverloadScores {
    double small_score = 1.0;
    double large_score = 1.0;
};

[[nodiscard]] BucketPathOverloadScores evaluate_bucket_path_overload(
    const BucketPathOverloadInput& input) noexcept;

struct SplitScannerCapacityDecision {
    std::size_t small_scanners = 1;
    std::size_t large_scanners = 1;
};

[[nodiscard]] SplitScannerCapacityDecision choose_split_scanner_capacity(
    std::size_t base_small_scanners,
    std::size_t base_large_scanners,
    std::size_t large_scanner_floor,
    std::uint32_t large_reader_small_priority_percent) noexcept;

struct DataHashBenchmarkReport {
    std::size_t files_found = 0;
    std::size_t folders_found = 0;
    std::size_t files_read = 0;
    std::size_t files_failed = 0;
    std::uint64_t logical_size_bytes = 0;
    std::uint64_t bytes_read = 0;
    std::uint64_t bytes_hashed = 0;
    double read_bytes_per_second = 0.0;
    double read_gigabits_per_second = 0.0;
    double hash_bytes_per_second = 0.0;
    double hash_gigabits_per_second = 0.0;
    double elapsed_seconds = 0.0;
    std::size_t meta_reader_threads = 0;
    std::size_t metadata_async_depth = 0;
    std::size_t data_reader_threads = 0;
    std::size_t data_outstanding_requests = 0;
    std::size_t small_file_async_window = 0;
    std::size_t hash_worker_threads = 0;
    std::size_t max_files_queued = 0;
    std::size_t data_buffer_slots = 0;
    std::size_t data_queue_depth = 0;
    std::size_t hash_work_factor = 1;
    std::string hash_algorithm;
    bool pack_small_files = false;
    bool meta_reader_async = false;
    bool data_reader_async = false;
};

struct HashInventoryReport {
    std::size_t files_found = 0;
    std::size_t folders_found = 0;
    std::size_t files_hashed = 0;
    std::size_t files_failed = 0;
    std::uint64_t logical_size_bytes = 0;
    std::uint64_t bytes_read = 0;
    std::uint64_t bytes_hashed = 0;
    double read_bytes_per_second = 0.0;
    double read_gigabits_per_second = 0.0;
    double bytes_per_second = 0.0;
    double read_elapsed_seconds = 0.0;
    double elapsed_seconds = 0.0;
    std::size_t meta_reader_threads = 0;
    std::size_t metadata_async_depth = 0;
    std::size_t data_reader_threads = 0;
    std::size_t data_outstanding_requests = 0;
    std::size_t hash_worker_threads = 0;
    std::size_t max_files_queued = 0;
    std::size_t max_hash_chunks_queued = 0;
    std::uint64_t hash_block_size = 0;
    std::size_t metadata_files_written = 0;
    std::size_t metadata_folders_written = 0;
    std::string hash_mode;
    std::string hash_algorithm;
    bool meta_reader_async = false;
    bool data_reader_async = false;
};

struct HashSpeedBenchmarkReport {
    std::uint64_t bytes_hashed = 0;
    std::uint64_t iterations = 0;
    std::uint64_t digest_mix = 0;
    double bytes_per_second = 0.0;
    double gigabits_per_second = 0.0;
    double bytes_per_core_second = 0.0;
    double gigabits_per_core_second = 0.0;
    double gigabits_per_hardware_core_second = 0.0;
    double elapsed_seconds = 0.0;
    double duration_seconds = 0.0;
    double min_gigabits_per_core_second = 0.0;
    std::size_t worker_threads = 0;
    std::size_t hardware_threads = 0;
    std::uint64_t block_size = 0;
    std::string hash_algorithm;
    bool passed = true;
};

struct MetadataWriterBenchmarkReport {
    std::uint64_t files_generated = 0;
    std::uint64_t folders_generated = 0;
    std::uint64_t records_written = 0;
    std::uint64_t logical_size_bytes = 0;
    double elapsed_seconds = 0.0;
    double records_per_second = 0.0;
    double files_per_second = 0.0;
    std::size_t batch_size = 0;
    std::size_t partitions = 1;
    std::string partition_mode = "threads";
    std::string output_format;
    std::filesystem::path output_path;
};

struct BufferTransportBenchmarkReport {
    std::uint64_t buffers_generated = 0;
    std::uint64_t buffers_sent = 0;
    std::uint64_t buffers_received = 0;
    std::uint64_t buffers_discarded = 0;
    std::uint64_t payload_bytes_sent = 0;
    std::uint64_t payload_bytes_received = 0;
    double elapsed_seconds = 0.0;
    double bytes_per_second = 0.0;
    double gigabytes_per_second = 0.0;
    double gibibytes_per_second = 0.0;
    double gigabits_per_second = 0.0;
    std::size_t transports = 1;
    std::size_t generator_threads = 1;
    std::size_t sender_threads = 1;
    std::size_t receiver_threads = 1;
    std::size_t discarder_threads = 1;
    std::size_t buffer_size = 0;
    std::size_t pool_slots_per_transport = 0;
    std::uint64_t buffers_per_transport = 0;
    std::string pattern;
    std::string transport_kind;
};

struct FakeRemoteDiffBenchmarkReport {
    std::uint64_t source_files_generated = 0;
    std::uint64_t source_folders_generated = 0;
    std::uint64_t target_folders_checked = 0;
    std::uint64_t files_compared = 0;
    std::uint64_t files_same = 0;
    std::uint64_t bytes_compared = 0;
    double source_elapsed_seconds = 0.0;
    double total_elapsed_seconds = 0.0;
    double source_records_per_second = 0.0;
    double total_records_per_second = 0.0;
    std::size_t source_threads = 1;
    std::size_t fake_remote_threads = 1;
    std::uint64_t remote_delay_microseconds = 0;
    std::size_t request_queue_depth = 0;
    std::size_t batch_queue_depth = 0;
    double source_wait_target_queue_seconds = 0.0;
    double source_wait_batch_queue_seconds = 0.0;
    double fake_remote_wait_request_seconds = 0.0;
    double fake_remote_wait_processor_queue_seconds = 0.0;
    double fake_remote_delay_seconds = 0.0;
    double fake_remote_wait_batch_queue_seconds = 0.0;
    double joiner_idle_seconds = 0.0;
    double joiner_process_seconds = 0.0;
    std::size_t checker_threads = 1;
};

struct DistributedDiffRunReport {
    std::uint64_t folders_sent = 0;
    std::uint64_t folders_reported = 0;
    std::uint64_t files_compared = 0;
    std::uint64_t files_same = 0;
    std::uint64_t files_changed = 0;
    std::uint64_t files_new = 0;
    std::uint64_t files_target_only = 0;
    std::uint64_t files_failed = 0;
    std::uint64_t source_logical_size_bytes = 0;
    std::uint64_t target_logical_size_bytes = 0;
    std::uint64_t bytes_planned = 0;
    double elapsed_seconds = 0.0;
    double folders_per_second = 0.0;
    double files_per_second = 0.0;
};

[[nodiscard]] SenderRuntimeConfig load_sender_runtime_config(const ConfigStore& config);
[[nodiscard]] ReceiverRuntimeConfig load_receiver_runtime_config(const ConfigStore& config);

class TransferEngine {
public:
    explicit TransferEngine(EngineConfig config = {}, ConfigStore config_store = {});
    [[nodiscard]] TransferReport run(const std::vector<FileSpec>& source_files,
                                     const ScanIndex* source_scan = nullptr,
                                     const ScanIndex* target_scan = nullptr) const;
    [[nodiscard]] TransferReport scan_directory_report(const std::filesystem::path& source_root,
                                                       char scan_side = 'S',
                                                       bool recursive = true) const;
    [[nodiscard]] TransferReport dry_run_directory(const std::filesystem::path& source_root,
                                                   const ScanIndex* source_scan = nullptr,
                                                   const ScanIndex* target_scan = nullptr,
                                                   bool recursive = true) const;
    [[nodiscard]] TransferReport diff_scan_indexes(const ScanIndex& source_scan,
                                                   const ScanIndex& target_scan,
                                                   const std::string& compare_mode = "time") const;
    [[nodiscard]] TransferReport diff_metadata_trees(const std::filesystem::path& source_root,
                                                     const std::filesystem::path& target_root,
                                                     const std::string& compare_mode = "time",
                                                     bool recursive = true,
                                                     std::size_t meta_reader_threads = 0,
                                                     std::size_t metadata_async_depth = 0,
                                                     double max_duration_seconds = 0.0,
                                                     bool collect_detailed_records = true,
                                                     std::uint32_t stats_interval_seconds = 0,
                                                     std::size_t checker_threads = 0,
                                                     std::size_t checker_request_queue_depth = 0,
                                                     std::size_t checker_batch_queue_depth = 0) const;
    [[nodiscard]] ScanIndex build_scan_index(const std::filesystem::path& source_root,
                                             char scan_side = 'S',
                                             bool recursive = true) const;
    [[nodiscard]] MetadataBenchmarkReport benchmark_metadata_pipeline(const std::filesystem::path& source_root,
                                                                     bool recursive = true,
                                                                     bool discard_after_checker = true,
                                                                     bool use_stats_discarder = false,
                                                                     std::size_t meta_reader_threads = 0,
                                                                     std::size_t metadata_async_depth = 0,
                                                                     const std::filesystem::path& metadata_output_path = {},
                                                                     const std::string& metadata_output_format = {},
                                                                     const std::string& metadata_records = {},
                                                                     double max_duration_seconds = 0.0,
                                                                     std::uint32_t stats_interval_seconds = 5,
                                                                     std::size_t record_buffer_slots = 0,
                                                                     std::size_t metadata_output_partitions = 1,
                                                                     const std::string& metadata_output_partition_mode = "single",
                                                                     const std::filesystem::path& status_socket_path = {},
                                                                     bool pipeline_autoscale = false,
                                                                     std::string autoscale_profile = {},
                                                                     std::filesystem::path autoscale_settings_path = {},
                                                                     std::uint64_t autoscale_interval_ms = 1000) const;
    [[nodiscard]] DataReadBenchmarkReport benchmark_data_read_pipeline(const std::filesystem::path& source_root,
                                                                       bool recursive = true,
                                                                       std::size_t meta_reader_threads = 0,
                                                                       std::size_t metadata_async_depth = 0,
                                                                       std::size_t readdirplus_page_bytes = 0,
                                                                       std::size_t data_reader_threads = 0,
                                                                       std::size_t data_outstanding_requests = 0,
                                                                       std::size_t small_file_async_window = 0,
                                                                       bool split_small_large = false,
                                                                       bool dual_scan_small_large = false,
                                                                       bool recon_scan_enabled = false,
                                                                       bool morph_large_readers_to_small = false,
                                                                       bool bucket_priority_enabled = false,
                                                                       std::uint64_t split_small_file_threshold = 0,
                                                                       std::size_t recon_meta_reader_threads = 0,
                                                                       std::size_t recon_metadata_async_depth = 0,
                                                                       std::uint64_t recon_page_sleep_us = 0,
                                                                       std::size_t small_meta_reader_threads = 0,
                                                                       std::size_t large_meta_reader_threads = 0,
                                                                       std::size_t small_data_reader_threads = 0,
                                                                       std::size_t large_data_reader_threads = 0,
                                                                       std::size_t large_data_outstanding_requests = 0,
                                                                       bool pipeline_autoscale = false,
                                                                     bool large_reader_autoscale = false,
                                                                     std::size_t large_reader_initial_threads = 0,
                                                                     std::uint64_t autoscale_interval_ms = 1000,
                                                                     std::string autoscale_profile = {},
                                                                     std::filesystem::path autoscale_settings_path = {},
                                                                     std::uint64_t max_file_size_bytes = 0,
                                                                       std::size_t max_files_queued = 1024,
                                                                       std::size_t small_max_files_queued = 0,
                                                                       std::size_t large_max_files_queued = 0,
                                                                       std::size_t data_buffer_slots = 0,
                                                                       std::size_t data_queue_depth = 0,
                                                                       const std::string& data_copy_mode = {},
                                                                       bool pack_small_files = false,
                                                                       double max_duration_seconds = 0.0,
                                                                       std::uint32_t stats_interval_seconds = 5,
                                                                       const std::filesystem::path& status_socket_path = {}) const;
    [[nodiscard]] DataReadBenchmarkReport benchmark_data_write_pipeline(const std::filesystem::path& source_root,
                                                                        const std::string& target_root,
                                                                        bool recursive = true,
                                                                        std::size_t meta_reader_threads = 0,
                                                                        std::size_t metadata_async_depth = 0,
                                                                        std::size_t readdirplus_page_bytes = 0,
                                                                        std::size_t data_reader_threads = 0,
                                                                        std::size_t data_writer_threads = 0,
                                                                        std::size_t data_writer_async_window = 0,
                                                                        std::size_t data_outstanding_requests = 0,
                                                                        std::size_t small_file_async_window = 0,
                                                                        std::uint64_t min_file_size_bytes = 0,
                                                                        std::uint64_t max_file_size_bytes = 0,
                                                                        std::size_t max_files_queued = 1024,
                                                                        std::size_t data_buffer_slots = 0,
                                                                        std::size_t data_queue_depth = 0,
                                                                        const std::string& data_copy_mode = {},
                                                                        bool pack_small_files = false,
                                                                        double max_duration_seconds = 0.0,
                                                                        std::uint32_t stats_interval_seconds = 5,
                                                                        bool verify_hash = false,
                                                                        bool preserve_target_metadata = true,
                                                                        bool target_fsync = true,
                                                                        bool ensure_target_directories = true,
                                                                        bool stable_small_file_writes = false,
                                                                        bool tcp_cork_small_file_writes = false,
                                                                        bool direct_reactor_writes = false,
                                                                        bool direct_reactor_submit = false,
                                                                        std::size_t data_writer_reactors = 0,
                                                                        std::size_t reactors_per_ip = 1,
                                                                        std::size_t data_writer_file_window = 0,
                                                                        bool mkdir_only = false,
                                                                        bool folder_ready_discard = false,
                                                                        bool folder_ready_write = false,
                                                                        bool folder_ready_mixed_write = false,
                                                                        std::string small_file_target_ips = {},
                                                                        std::string large_file_target_ips = {}) const;
    [[nodiscard]] DataReadBenchmarkReport benchmark_nfs_open_pipeline(const std::filesystem::path& source_root,
                                                                      bool recursive = true,
                                                                      std::size_t meta_reader_threads = 0,
                                                                      std::size_t metadata_async_depth = 0,
                                                                      std::size_t open_threads = 0,
                                                                      std::size_t max_files_queued = 1024,
                                                                      double max_duration_seconds = 0.0,
                                                                      std::uint32_t stats_interval_seconds = 5) const;
    [[nodiscard]] DataHashBenchmarkReport benchmark_data_hash_pipeline(const std::filesystem::path& source_root,
                                                                       bool recursive = true,
                                                                       const std::string& hash_algorithm = {},
                                                                       std::size_t meta_reader_threads = 0,
                                                                       std::size_t metadata_async_depth = 0,
                                                                       std::size_t data_reader_threads = 0,
                                                                       std::size_t data_outstanding_requests = 0,
                                                                       std::size_t small_file_async_window = 0,
                                                                       std::size_t hash_worker_threads = 0,
                                                                       std::size_t max_files_queued = 1024,
                                                                       std::size_t data_buffer_slots = 0,
                                                                       std::size_t data_queue_depth = 0,
                                                                       std::size_t hash_work_factor = 0,
                                                                       bool pack_small_files = false,
                                                                       double max_duration_seconds = 0.0,
                                                                       std::uint32_t stats_interval_seconds = 5,
                                                                       const std::filesystem::path& status_socket_path = {}) const;
    [[nodiscard]] HashInventoryReport hash_inventory_pipeline(const std::filesystem::path& source_root,
                                                              bool recursive = true,
                                                              const std::string& hash_algorithm = {},
                                                              std::size_t meta_reader_threads = 0,
                                                              std::size_t metadata_async_depth = 0,
                                                              std::size_t data_reader_threads = 0,
                                                              std::size_t data_outstanding_requests = 0,
                                                              std::size_t hash_worker_threads = 0,
                                                              std::size_t max_files_queued = 1024,
                                                              std::size_t max_hash_chunks_queued = 4096,
                                                              const std::string& hash_mode = "file",
                                                              std::uint64_t hash_block_size = 1024 * 1024,
                                                              const std::filesystem::path& metadata_output_path = {},
                                                              const std::string& metadata_output_format = {},
                                                              const std::string& metadata_records = "all",
                                                              double max_duration_seconds = 0.0) const;
    [[nodiscard]] HashSpeedBenchmarkReport benchmark_hash_speed(const std::string& hash_algorithm = "xxh64",
                                                                std::size_t worker_threads = 1,
                                                                std::uint64_t block_size = 1024 * 1024,
                                                                double duration_seconds = 5.0,
                                                                double min_gigabits_per_core_second = 0.0) const;
    [[nodiscard]] MetadataWriterBenchmarkReport benchmark_metadata_writer(
        const std::filesystem::path& output_path,
        const std::string& output_format = "parquet",
        std::uint64_t file_count = 1'000'000,
        std::uint64_t folder_count = 1'000,
        std::size_t batch_size = 65'536,
        std::uint64_t average_file_size = 32 * 1024,
        const std::string& duckdb_memory_limit = {},
        std::size_t duckdb_threads = 0,
        const std::string& duckdb_checkpoint_threshold = {},
        const std::string& parquet_compression = {},
        std::size_t partitions = 1,
        const std::string& partition_mode = "threads") const;
    [[nodiscard]] BufferTransportBenchmarkReport benchmark_buffer_transport(
        std::size_t transports = 1,
        std::uint64_t buffers_per_transport = 1024,
        std::size_t buffer_size = 1024 * 1024,
        std::size_t pool_slots_per_transport = 256,
        std::size_t generator_threads = 1,
        std::size_t sender_threads = 1,
        std::size_t receiver_threads = 1,
        std::size_t discarder_threads = 1,
        const std::string& pattern = "xoshiro256",
        const std::string& transport_kind = "unix",
        std::uint16_t base_port = 39000,
        const std::filesystem::path& socket_dir = {},
        bool shared_input_queue = false) const;
    [[nodiscard]] FakeRemoteDiffBenchmarkReport benchmark_fake_remote_diff_pipeline(
        std::uint64_t file_count = 1'000'000,
        std::uint64_t folder_count = 1'000,
        std::uint64_t average_file_size = 32 * 1024,
        std::size_t source_threads = 1,
        std::size_t fake_remote_threads = 1,
        std::uint64_t remote_delay_microseconds = 0,
        std::size_t request_queue_depth = 0,
        std::size_t batch_queue_depth = 0,
        std::uint32_t stats_interval_seconds = 0,
        std::size_t checker_threads = 0) const;
    [[nodiscard]] DistributedDiffRunReport run_distributed_diff_source(
        const std::filesystem::path& source_root,
        const std::string& target_host,
        std::uint16_t target_port,
        const std::filesystem::path& folder_report_path,
        const std::string& compare_mode = "size",
        bool recursive = true,
        std::size_t meta_reader_threads = 0,
        std::size_t metadata_async_depth = 0,
        double max_duration_seconds = 0.0,
        std::uint32_t stats_interval_seconds = 5,
        bool pipeline_autoscale = false,
        std::string autoscale_profile = {},
        std::filesystem::path autoscale_settings_path = {},
        std::uint64_t autoscale_interval_ms = 1000) const;
    void run_distributed_diff_target(const std::filesystem::path& target_root,
                                     const std::string& listen_host,
                                     std::uint16_t listen_port,
                                     const std::string& compare_mode = "size",
                                     bool recursive = true,
                                     std::size_t target_threads = 0,
                                     std::size_t metadata_async_depth = 0,
                                     std::uint32_t stats_interval_seconds = 5,
                                     bool pipeline_autoscale = false,
                                     std::string autoscale_profile = {},
                                     std::filesystem::path autoscale_settings_path = {},
                                     std::uint64_t autoscale_interval_ms = 1000) const;
    [[nodiscard]] TransferReport transfer_directory(const SenderRuntimeConfig& runtime) const;
    void run_receiver(const ReceiverRuntimeConfig& runtime) const;
    [[nodiscard]] static std::vector<FileSpec> scan_directory(const std::filesystem::path& source_root, bool recursive = true);
    static ScanIndex load_scan_csv(const std::filesystem::path& input_path);
    static void write_scan_csv(const ScanIndex& index, const std::filesystem::path& output_path);
    static void write_diff_csv(const TransferReport& report, const std::filesystem::path& output_path);

private:
    [[nodiscard]] std::vector<FileSpec> scan_directory_with_config(const std::filesystem::path& source_root,
                                                                   bool recursive) const;

    EngineConfig config_;
    ConfigStore config_store_;
};

}  // namespace hypersync

#endif
