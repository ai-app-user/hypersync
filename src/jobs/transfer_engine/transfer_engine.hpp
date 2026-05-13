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
    std::uint32_t stats_interval_seconds = 0;
    std::string scan_run_id;
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
    double bytes_per_second = 0.0;
    double gigabits_per_second = 0.0;
    double elapsed_seconds = 0.0;
    std::size_t meta_reader_threads = 0;
    std::size_t metadata_async_depth = 0;
    std::size_t data_reader_threads = 0;
    std::size_t data_outstanding_requests = 0;
    std::size_t max_files_queued = 0;
    std::size_t data_buffer_slots = 0;
    std::size_t data_queue_depth = 0;
    std::string data_copy_mode;
    bool meta_reader_async = false;
    bool data_reader_async = false;
};

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
    std::size_t hash_worker_threads = 0;
    std::size_t max_files_queued = 0;
    std::size_t data_buffer_slots = 0;
    std::size_t data_queue_depth = 0;
    std::size_t hash_work_factor = 1;
    std::string hash_algorithm;
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
                                                                     const std::filesystem::path& status_socket_path = {}) const;
    [[nodiscard]] DataReadBenchmarkReport benchmark_data_read_pipeline(const std::filesystem::path& source_root,
                                                                       bool recursive = true,
                                                                       std::size_t meta_reader_threads = 1,
                                                                       std::size_t metadata_async_depth = 16,
                                                                       std::size_t data_reader_threads = 0,
                                                                       std::size_t data_outstanding_requests = 0,
                                                                       std::size_t max_files_queued = 1024,
                                                                       std::size_t data_buffer_slots = 0,
                                                                       std::size_t data_queue_depth = 0,
                                                                       const std::string& data_copy_mode = {},
                                                                       double max_duration_seconds = 0.0,
                                                                       std::uint32_t stats_interval_seconds = 5,
                                                                       const std::filesystem::path& status_socket_path = {}) const;
    [[nodiscard]] DataHashBenchmarkReport benchmark_data_hash_pipeline(const std::filesystem::path& source_root,
                                                                       bool recursive = true,
                                                                       const std::string& hash_algorithm = "xxh64",
                                                                       std::size_t meta_reader_threads = 1,
                                                                       std::size_t metadata_async_depth = 16,
                                                                       std::size_t data_reader_threads = 0,
                                                                       std::size_t data_outstanding_requests = 0,
                                                                       std::size_t hash_worker_threads = 0,
                                                                       std::size_t max_files_queued = 1024,
                                                                       std::size_t data_buffer_slots = 0,
                                                                       std::size_t data_queue_depth = 0,
                                                                       std::size_t hash_work_factor = 1,
                                                                       double max_duration_seconds = 0.0,
                                                                       std::uint32_t stats_interval_seconds = 5,
                                                                       const std::filesystem::path& status_socket_path = {}) const;
    [[nodiscard]] HashInventoryReport hash_inventory_pipeline(const std::filesystem::path& source_root,
                                                              bool recursive = true,
                                                              const std::string& hash_algorithm = "xxh64",
                                                              std::size_t meta_reader_threads = 1,
                                                              std::size_t metadata_async_depth = 16,
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
