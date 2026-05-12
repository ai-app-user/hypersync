#ifndef HYPERSYNC_CORE_METADATA_RECORD_WRITER_HPP
#define HYPERSYNC_CORE_METADATA_RECORD_WRITER_HPP

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "common/types.hpp"

namespace hypersync {

class ConfigStore;
struct DuckDbParquetState;

enum class MetadataRecordFormat {
    text,
    csv,
    parquet,
};

struct MetadataScanRunInfo {
    std::string run_id;
    std::string started_at_utc;
    std::uint64_t started_unix_ns = 0;
    std::string source_root;
    std::string settings_json;
};

struct MetadataRecordWriterConfig {
    bool enabled;
    std::filesystem::path output_path;
    MetadataRecordFormat format;
    bool write_files;
    bool write_folders;
    bool flush_per_batch;
    std::string duckdb_memory_limit;
    std::size_t duckdb_threads = 0;
    std::string duckdb_checkpoint_threshold;
    std::string parquet_compression;
    std::optional<MetadataScanRunInfo> run_info;

    MetadataRecordWriterConfig();
    MetadataRecordWriterConfig(bool enabled,
                               std::filesystem::path output_path,
                               MetadataRecordFormat format,
                               bool write_files,
                               bool write_folders,
                               bool flush_per_batch);
    MetadataRecordWriterConfig(bool enabled,
                               std::filesystem::path output_path,
                               MetadataRecordFormat format,
                               bool write_files,
                               bool write_folders,
                               bool flush_per_batch,
                               std::string duckdb_memory_limit,
                               std::size_t duckdb_threads,
                               std::string duckdb_checkpoint_threshold,
                               std::string parquet_compression,
                               std::optional<MetadataScanRunInfo> run_info);
};

struct MetadataFolderRecord {
    FileSpec spec;
    std::size_t flat_file_count = 0;
    std::uint64_t flat_logical_size_bytes = 0;
};

[[nodiscard]] MetadataRecordFormat parse_metadata_record_format(const std::string& value);
[[nodiscard]] std::string to_string(MetadataRecordFormat format);
[[nodiscard]] MetadataRecordFormat infer_metadata_record_format(const std::filesystem::path& output_path);
[[nodiscard]] MetadataRecordWriterConfig load_metadata_record_writer_config(const ConfigStore& config);

class MetadataRecordWriter {
public:
    explicit MetadataRecordWriter(MetadataRecordWriterConfig config);
    ~MetadataRecordWriter();

    void write_batch(const std::vector<FileSpec>& files, const std::vector<MetadataFolderRecord>& folders);
    void close();
    [[nodiscard]] std::size_t files_written() const;
    [[nodiscard]] std::size_t folders_written() const;
    [[nodiscard]] const MetadataRecordWriterConfig& config() const;

private:
    void write_header();
    void write_file(const FileSpec& spec);
    void write_folder(const MetadataFolderRecord& folder);
    void write_parquet_batch(const std::vector<FileSpec>& files, const std::vector<MetadataFolderRecord>& folders);

    mutable std::mutex mutex_;
    MetadataRecordWriterConfig config_;
    std::ofstream output_;
    std::unique_ptr<DuckDbParquetState> parquet_;
    std::size_t files_written_ = 0;
    std::size_t folders_written_ = 0;
    bool closed_ = false;
};

}  // namespace hypersync

#endif
