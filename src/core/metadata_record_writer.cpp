#include "core/metadata_record_writer.hpp"

#include <cctype>
#include <algorithm>
#include <array>
#include <cstdint>
#include <thread>
#include <iomanip>
#include <cstdio>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "common/config.hpp"
#include "common/path_utils.hpp"

#ifndef HYPERSYNC_HAS_DUCKDB
#define HYPERSYNC_HAS_DUCKDB 0
#endif

#if HYPERSYNC_HAS_DUCKDB
#include <duckdb.h>
#endif

namespace hypersync {

namespace {

std::string csv_quote(std::string_view value) {
    bool needs_quotes = value.empty();
    for (const char ch : value) {
        if (ch == ',' || ch == '"' || ch == '\n' || ch == '\r') {
            needs_quotes = true;
            break;
        }
    }
    if (!needs_quotes) {
        return std::string(value);
    }

    std::string out;
    out.reserve(value.size() + 2);
    out.push_back('"');
    for (const char ch : value) {
        if (ch == '"') {
            out.push_back('"');
        }
        out.push_back(ch);
    }
    out.push_back('"');
    return out;
}

std::string text_quote(std::string_view value) {
    std::ostringstream out;
    out << std::quoted(std::string(value));
    return out.str();
}

std::string extension_lower(std::filesystem::path path) {
    std::string extension = path.extension().string();
    for (char& ch : extension) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    return extension;
}

std::uint64_t logical_size(const FileSpec& spec) {
    return spec.declared_size != 0 ? spec.declared_size : spec.content.size();
}

bool path_needs_normalize(std::string_view path) {
    if (path.empty()) {
        return false;
    }
    if (path.front() == '/' || path.back() == '/') {
        return true;
    }
    return path.find("//") != std::string_view::npos;
}

std::string_view normalized_path_view(std::string_view path, std::string& scratch) {
    if (!path_needs_normalize(path)) {
        return path;
    }
    scratch = normalize_path(path);
    return scratch;
}

void write_csv_run_fields(std::ostream& output, const std::optional<MetadataScanRunInfo>& run_info) {
    if (!run_info.has_value()) {
        output << ",,,,,";
        return;
    }
    output << ','
           << csv_quote(run_info->run_id) << ','
           << csv_quote(run_info->started_at_utc) << ','
           << run_info->started_unix_ns << ','
           << csv_quote(run_info->source_root) << ','
           << csv_quote(run_info->settings_json);
}

#if HYPERSYNC_HAS_DUCKDB
constexpr idx_t kMetadataColumnCount = 20;

std::string sql_quote(std::string_view value) {
    std::string out;
    out.reserve(value.size() + 2);
    out.push_back('\'');
    for (const char ch : value) {
        if (ch == '\'') {
            out.push_back('\'');
        }
        out.push_back(ch);
    }
    out.push_back('\'');
    return out;
}

void check_duckdb_state(duckdb_state state, std::string_view operation) {
    if (state == DuckDBError) {
        throw std::runtime_error(std::string(operation) + " failed");
    }
}

void execute_duckdb_query(duckdb_connection connection, const std::string& sql, std::string_view operation) {
    duckdb_result result {};
    if (duckdb_query(connection, sql.c_str(), &result) == DuckDBError) {
        std::string error = duckdb_result_error(&result) != nullptr ? duckdb_result_error(&result)
                                                                    : "unknown DuckDB error";
        duckdb_destroy_result(&result);
        throw std::runtime_error(std::string(operation) + ": " + error);
    }
    duckdb_destroy_result(&result);
}

void append_optional_varchar(duckdb_appender appender, const std::string& value) {
    if (value.empty()) {
        check_duckdb_state(duckdb_append_null(appender), "duckdb_append_null");
        return;
    }
    check_duckdb_state(duckdb_append_varchar(appender, value.c_str()), "duckdb_append_varchar");
}

void append_run_fields(duckdb_appender appender, const std::optional<MetadataScanRunInfo>& run_info) {
    if (!run_info.has_value()) {
        check_duckdb_state(duckdb_append_null(appender), "duckdb_append_null");
        check_duckdb_state(duckdb_append_null(appender), "duckdb_append_null");
        check_duckdb_state(duckdb_append_null(appender), "duckdb_append_null");
        check_duckdb_state(duckdb_append_null(appender), "duckdb_append_null");
        check_duckdb_state(duckdb_append_null(appender), "duckdb_append_null");
        return;
    }

    append_optional_varchar(appender, run_info->run_id);
    append_optional_varchar(appender, run_info->started_at_utc);
    check_duckdb_state(duckdb_append_int64(appender, static_cast<int64_t>(run_info->started_unix_ns)),
                       "duckdb_append_int64");
    append_optional_varchar(appender, run_info->source_root);
    append_optional_varchar(appender, run_info->settings_json);
}

void destroy_logical_types(std::array<duckdb_logical_type, kMetadataColumnCount>& types) {
    for (duckdb_logical_type& type : types) {
        if (type != nullptr) {
            duckdb_destroy_logical_type(&type);
        }
    }
}

std::array<duckdb_logical_type, kMetadataColumnCount> make_metadata_column_types() {
    std::array<duckdb_logical_type, kMetadataColumnCount> types {};
    types[0] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    types[1] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    types[2] = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
    types[3] = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
    types[4] = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    types[5] = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    types[6] = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    types[7] = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
    types[8] = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
    types[9] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    types[10] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    types[11] = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
    types[12] = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
    types[13] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    types[14] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    types[15] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    types[16] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    types[17] = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
    types[18] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    types[19] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    return types;
}

duckdb_data_chunk make_metadata_data_chunk() {
    auto types = make_metadata_column_types();
    duckdb_data_chunk chunk = duckdb_create_data_chunk(types.data(), kMetadataColumnCount);
    destroy_logical_types(types);
    if (chunk == nullptr) {
        throw std::runtime_error("failed to create DuckDB metadata data chunk");
    }
    return chunk;
}

duckdb_vector vector_at(duckdb_data_chunk chunk, idx_t column) {
    return duckdb_data_chunk_get_vector(chunk, column);
}

void set_null(duckdb_data_chunk chunk, idx_t column, idx_t row) {
    duckdb_vector vector = vector_at(chunk, column);
    duckdb_vector_ensure_validity_writable(vector);
    duckdb_validity_set_row_invalid(duckdb_vector_get_validity(vector), row);
}

void set_varchar(duckdb_data_chunk chunk, idx_t column, idx_t row, std::string_view value) {
    duckdb_vector_assign_string_element_len(vector_at(chunk, column),
                                            row,
                                            value.data(),
                                            static_cast<idx_t>(value.size()));
}

void set_i64(duckdb_data_chunk chunk, idx_t column, idx_t row, std::int64_t value) {
    auto* values = static_cast<std::int64_t*>(duckdb_vector_get_data(vector_at(chunk, column)));
    values[row] = value;
}

void set_i32(duckdb_data_chunk chunk, idx_t column, idx_t row, std::int32_t value) {
    auto* values = static_cast<std::int32_t*>(duckdb_vector_get_data(vector_at(chunk, column)));
    values[row] = value;
}

void set_run_fields(duckdb_data_chunk chunk,
                    idx_t row,
                    const std::optional<MetadataScanRunInfo>& run_info) {
    if (!run_info.has_value()) {
        for (idx_t column = 15; column <= 19; ++column) {
            set_null(chunk, column, row);
        }
        return;
    }

    set_varchar(chunk, 15, row, run_info->run_id);
    set_varchar(chunk, 16, row, run_info->started_at_utc);
    set_i64(chunk, 17, row, static_cast<std::int64_t>(run_info->started_unix_ns));
    set_varchar(chunk, 18, row, run_info->source_root);
    set_varchar(chunk, 19, row, run_info->settings_json);
}
#endif

}  // namespace

struct DuckDbParquetState {
#if HYPERSYNC_HAS_DUCKDB
    duckdb_database database = nullptr;
    duckdb_connection connection = nullptr;
    duckdb_appender appender = nullptr;
    duckdb_data_chunk chunk = nullptr;
    idx_t chunk_capacity = 0;
#endif
    std::filesystem::path temp_database_path;
    std::filesystem::path temp_output_path;
    std::filesystem::path output_path;
    bool appender_closed = false;
};

MetadataRecordWriterConfig::MetadataRecordWriterConfig()
    : MetadataRecordWriterConfig(load_metadata_record_writer_config(ConfigStore{})) {}

MetadataRecordWriterConfig::MetadataRecordWriterConfig(bool enabled,
                                                       std::filesystem::path output_path,
                                                       MetadataRecordFormat format,
                                                       bool write_files,
                                                       bool write_folders,
                                                       bool flush_per_batch)
    : MetadataRecordWriterConfig(enabled,
                                 std::move(output_path),
                                 format,
                                 write_files,
                                 write_folders,
                                 flush_per_batch,
                                 "32GB",
                                 std::max<unsigned>(1U, std::thread::hardware_concurrency()),
                                 "2GB",
                                 "zstd",
                                 std::nullopt) {}

MetadataRecordWriterConfig::MetadataRecordWriterConfig(bool enabled,
                                                       std::filesystem::path output_path,
                                                       MetadataRecordFormat format,
                                                       bool write_files,
                                                       bool write_folders,
                                                       bool flush_per_batch,
                                                       std::string duckdb_memory_limit,
                                                       std::size_t duckdb_threads,
                                                       std::string duckdb_checkpoint_threshold,
                                                       std::string parquet_compression,
                                                       std::optional<MetadataScanRunInfo> run_info)
    : enabled(enabled),
      output_path(std::move(output_path)),
      format(format),
      write_files(write_files),
      write_folders(write_folders),
      flush_per_batch(flush_per_batch),
      duckdb_memory_limit(std::move(duckdb_memory_limit)),
      duckdb_threads(duckdb_threads),
      duckdb_checkpoint_threshold(std::move(duckdb_checkpoint_threshold)),
      parquet_compression(std::move(parquet_compression)),
      run_info(std::move(run_info)) {}

MetadataRecordFormat parse_metadata_record_format(const std::string& value) {
    if (value == "text" || value == "txt") {
        return MetadataRecordFormat::text;
    }
    if (value == "csv") {
        return MetadataRecordFormat::csv;
    }
    if (value == "parquet") {
        return MetadataRecordFormat::parquet;
    }
    throw std::invalid_argument("metadata record format must be text, csv, or parquet");
}

std::string to_string(MetadataRecordFormat format) {
    switch (format) {
        case MetadataRecordFormat::text:
            return "text";
        case MetadataRecordFormat::csv:
            return "csv";
        case MetadataRecordFormat::parquet:
            return "parquet";
    }
    return "unknown";
}

MetadataRecordFormat infer_metadata_record_format(const std::filesystem::path& output_path) {
    const std::string extension = extension_lower(output_path);
    if (extension == ".csv") {
        return MetadataRecordFormat::csv;
    }
    if (extension == ".parquet") {
        return MetadataRecordFormat::parquet;
    }
    return MetadataRecordFormat::text;
}

MetadataRecordWriterConfig load_metadata_record_writer_config(const ConfigStore& config) {
    const ConfigSection values = config.merged_sections(default_job_config_sections("metadata_record_writer"));
    const std::filesystem::path output_path = config_string_or(values, "output_path", "");
    const std::string configured_format = config_string_or(values, "format", "auto");
    const MetadataRecordFormat format = configured_format == "auto" ? infer_metadata_record_format(output_path)
                                                                    : parse_metadata_record_format(configured_format);
    std::size_t duckdb_threads = config_size_t_or(values, "duckdb_threads", 0);
    if (duckdb_threads == 0U) {
        duckdb_threads = std::max<unsigned>(1U, std::thread::hardware_concurrency());
    }
    return MetadataRecordWriterConfig(config_bool_or(values, "enabled", false),
                                      output_path,
                                      format,
                                      config_bool_or(values, "write_files", true),
                                      config_bool_or(values, "write_folders", true),
                                      config_bool_or(values, "flush_per_batch", false),
                                      config_string_or(values, "duckdb_memory_limit", "32GB"),
                                      duckdb_threads,
                                      config_string_or(values, "duckdb_checkpoint_threshold", "2GB"),
                                      config_string_or(values, "parquet_compression", "zstd"),
                                      std::nullopt);
}

MetadataRecordWriter::MetadataRecordWriter(MetadataRecordWriterConfig config)
    : config_(std::move(config)) {
    if (!config_.enabled) {
        return;
    }
    if (config_.output_path.empty()) {
        throw std::invalid_argument("metadata record writer output path is required when enabled");
    }
    if (!config_.write_files && !config_.write_folders) {
        throw std::invalid_argument("metadata record writer must write files, folders, or both");
    }
    if (config_.format == MetadataRecordFormat::parquet) {
#if HYPERSYNC_HAS_DUCKDB
        parquet_ = std::make_unique<DuckDbParquetState>();
        parquet_->output_path = config_.output_path;
        parquet_->temp_database_path = config_.output_path;
        parquet_->temp_database_path += ".duckdb.tmp";
        parquet_->temp_output_path = config_.output_path;
        parquet_->temp_output_path += ".tmp";
        std::filesystem::remove(parquet_->temp_database_path);
        std::filesystem::remove(parquet_->temp_output_path);

        if (duckdb_open(parquet_->temp_database_path.string().c_str(), &parquet_->database) == DuckDBError) {
            throw std::runtime_error("failed to open temporary DuckDB database for parquet output");
        }
        if (duckdb_connect(parquet_->database, &parquet_->connection) == DuckDBError) {
            throw std::runtime_error("failed to connect to temporary DuckDB database for parquet output");
        }

        if (!config_.duckdb_memory_limit.empty()) {
            execute_duckdb_query(parquet_->connection,
                                 "SET memory_limit=" + sql_quote(config_.duckdb_memory_limit),
                                 "failed to set DuckDB memory_limit");
        }
        if (config_.duckdb_threads != 0U) {
            execute_duckdb_query(parquet_->connection,
                                 "SET threads=" + std::to_string(config_.duckdb_threads),
                                 "failed to set DuckDB threads");
        }
        if (!config_.duckdb_checkpoint_threshold.empty()) {
            execute_duckdb_query(parquet_->connection,
                                 "SET checkpoint_threshold=" + sql_quote(config_.duckdb_checkpoint_threshold),
                                 "failed to set DuckDB checkpoint_threshold");
        }
        execute_duckdb_query(parquet_->connection,
                             "PRAGMA disable_progress_bar",
                             "failed to disable DuckDB progress bar");

        const char* create_sql =
            "CREATE TABLE metadata_records ("
            "record_type VARCHAR, "
            "rel_path VARCHAR, "
            "size BIGINT, "
            "mtime BIGINT, "
            "mode INTEGER, "
            "uid INTEGER, "
            "gid INTEGER, "
            "flat_file_count BIGINT, "
            "flat_logical_size_bytes BIGINT, "
            "hash_algorithm VARCHAR, "
            "content_hash VARCHAR, "
            "hash_block_size BIGINT, "
            "hash_block_count BIGINT, "
            "block_hash_algorithm VARCHAR, "
            "block_hashes VARCHAR, "
            "scan_run_id VARCHAR, "
            "run_started_at_utc VARCHAR, "
            "run_started_unix_ns BIGINT, "
            "source_root VARCHAR, "
            "run_settings VARCHAR"
            ")";
        execute_duckdb_query(parquet_->connection, create_sql, "failed to create parquet staging table");

        if (std::filesystem::exists(parquet_->output_path)) {
            const std::string insert_sql =
                "INSERT INTO metadata_records SELECT "
                "record_type, rel_path, size, mtime, mode, uid, gid, flat_file_count, "
                "flat_logical_size_bytes, hash_algorithm, content_hash, hash_block_size, "
                "hash_block_count, block_hash_algorithm, block_hashes, scan_run_id, "
                "run_started_at_utc, run_started_unix_ns, source_root, run_settings "
                "FROM read_parquet(" +
                sql_quote(parquet_->output_path.string()) + ")";
            execute_duckdb_query(parquet_->connection,
                                 insert_sql,
                                 "failed to load existing parquet metadata output");
        }

        execute_duckdb_query(parquet_->connection, "BEGIN TRANSACTION", "failed to start DuckDB bulk transaction");
        check_duckdb_state(duckdb_appender_create(parquet_->connection,
                                                  nullptr,
                                                  "metadata_records",
                                                  &parquet_->appender),
                           "duckdb_appender_create");
        parquet_->chunk = make_metadata_data_chunk();
        parquet_->chunk_capacity = duckdb_vector_size();
        if (parquet_->chunk_capacity == 0U) {
            throw std::runtime_error("DuckDB reported zero vector capacity");
        }
        return;
#else
        throw std::runtime_error("metadata parquet output is not available in this build; rebuild with DuckDB");
#endif
    }

    output_.open(config_.output_path, std::ios::out | std::ios::trunc);
    if (!output_) {
        throw std::runtime_error("failed to open metadata output path: " + config_.output_path.string());
    }
    write_header();
}

MetadataRecordWriter::~MetadataRecordWriter() {
    try {
        close();
    } catch (...) {
    }
}

void MetadataRecordWriter::write_batch(const std::vector<FileSpec>& files,
                                       const std::vector<MetadataFolderRecord>& folders) {
    if (!config_.enabled) {
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (config_.format == MetadataRecordFormat::parquet) {
        write_parquet_batch(files, folders);
        files_written_ += config_.write_files ? files.size() : 0U;
        folders_written_ += config_.write_folders ? folders.size() : 0U;
        return;
    }
    if (config_.write_files) {
        for (const auto& file : files) {
            write_file(file);
            ++files_written_;
        }
    }
    if (config_.write_folders) {
        for (const auto& folder : folders) {
            write_folder(folder);
            ++folders_written_;
        }
    }
    if (config_.flush_per_batch) {
        output_.flush();
    }
}

void MetadataRecordWriter::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_) {
        return;
    }
    closed_ = true;

    if (!config_.enabled) {
        return;
    }

    if (config_.format != MetadataRecordFormat::parquet) {
        output_.flush();
        output_.close();
        return;
    }

#if HYPERSYNC_HAS_DUCKDB
    if (!parquet_) {
        return;
    }
    if (parquet_->appender != nullptr && !parquet_->appender_closed) {
        if (duckdb_appender_destroy(&parquet_->appender) == DuckDBError) {
            throw std::runtime_error("failed to finalize DuckDB parquet appender");
        }
        parquet_->appender_closed = true;
    }
    if (parquet_->chunk != nullptr) {
        duckdb_destroy_data_chunk(&parquet_->chunk);
    }

    execute_duckdb_query(parquet_->connection, "COMMIT", "failed to commit DuckDB bulk transaction");

    const std::string compression = config_.parquet_compression.empty() ? "zstd" : config_.parquet_compression;
    std::filesystem::remove(parquet_->temp_output_path);
    const std::string copy_sql =
        "COPY metadata_records TO " + sql_quote(parquet_->temp_output_path.string()) +
        " (FORMAT parquet, COMPRESSION " + compression + ")";
    execute_duckdb_query(parquet_->connection, copy_sql, "failed to write parquet metadata output");
    std::filesystem::rename(parquet_->temp_output_path, parquet_->output_path);

    duckdb_disconnect(&parquet_->connection);
    duckdb_close(&parquet_->database);
    std::filesystem::remove(parquet_->temp_database_path);
    parquet_.reset();
#endif
}

std::size_t MetadataRecordWriter::files_written() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return files_written_;
}

std::size_t MetadataRecordWriter::folders_written() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return folders_written_;
}

const MetadataRecordWriterConfig& MetadataRecordWriter::config() const {
    return config_;
}

void MetadataRecordWriter::write_header() {
    if (config_.format == MetadataRecordFormat::csv) {
        output_ << "record_type,rel_path,size,mtime,mode,uid,gid,flat_file_count,flat_logical_size_bytes,"
                   "hash_algorithm,content_hash,hash_block_size,hash_block_count,block_hash_algorithm,block_hashes,"
                   "scan_run_id,run_started_at_utc,run_started_unix_ns,source_root,run_settings\n";
    }
}

void MetadataRecordWriter::write_parquet_batch(const std::vector<FileSpec>& files,
                                               const std::vector<MetadataFolderRecord>& folders) {
#if HYPERSYNC_HAS_DUCKDB
    if (!parquet_ || parquet_->chunk == nullptr || parquet_->appender == nullptr) {
        throw std::logic_error("parquet writer is not initialized");
    }

    idx_t row = 0;
    const auto flush = [&]() {
        if (row == 0U) {
            return;
        }
        duckdb_data_chunk_set_size(parquet_->chunk, row);
        check_duckdb_state(duckdb_append_data_chunk(parquet_->appender, parquet_->chunk),
                           "duckdb_append_data_chunk");
        duckdb_data_chunk_reset(parquet_->chunk);
        row = 0;
    };
    const auto reserve_row = [&]() -> idx_t {
        if (row == parquet_->chunk_capacity) {
            flush();
        }
        return row++;
    };

    if (config_.write_files) {
        for (const auto& spec : files) {
            const idx_t current = reserve_row();
            std::string normalized;
            const std::string_view rel_path = normalized_path_view(spec.rel_path, normalized);
            set_varchar(parquet_->chunk, 0, current, "file");
            set_varchar(parquet_->chunk, 1, current, rel_path);
            set_i64(parquet_->chunk, 2, current, static_cast<std::int64_t>(logical_size(spec)));
            set_i64(parquet_->chunk, 3, current, static_cast<std::int64_t>(spec.mtime));
            set_i32(parquet_->chunk, 4, current, static_cast<std::int32_t>(spec.mode));
            set_i32(parquet_->chunk, 5, current, static_cast<std::int32_t>(spec.uid));
            set_i32(parquet_->chunk, 6, current, static_cast<std::int32_t>(spec.gid));
            set_null(parquet_->chunk, 7, current);
            set_null(parquet_->chunk, 8, current);
            set_varchar(parquet_->chunk, 9, current, spec.hash_algorithm);
            set_varchar(parquet_->chunk, 10, current, spec.content_hash);
            set_i64(parquet_->chunk, 11, current, static_cast<std::int64_t>(spec.hash_block_size));
            set_i64(parquet_->chunk, 12, current, static_cast<std::int64_t>(spec.hash_block_count));
            set_varchar(parquet_->chunk, 13, current, spec.block_hash_algorithm);
            set_varchar(parquet_->chunk, 14, current, spec.block_hashes);
            set_run_fields(parquet_->chunk, current, config_.run_info);
        }
    }

    if (config_.write_folders) {
        for (const auto& folder : folders) {
            const idx_t current = reserve_row();
            std::string normalized;
            const std::string_view rel_path = normalized_path_view(folder.spec.rel_path, normalized);
            set_varchar(parquet_->chunk, 0, current, "folder");
            set_varchar(parquet_->chunk, 1, current, rel_path);
            set_null(parquet_->chunk, 2, current);
            set_i64(parquet_->chunk, 3, current, static_cast<std::int64_t>(folder.spec.mtime));
            set_i32(parquet_->chunk, 4, current, static_cast<std::int32_t>(folder.spec.mode));
            set_i32(parquet_->chunk, 5, current, static_cast<std::int32_t>(folder.spec.uid));
            set_i32(parquet_->chunk, 6, current, static_cast<std::int32_t>(folder.spec.gid));
            set_i64(parquet_->chunk, 7, current, static_cast<std::int64_t>(folder.flat_file_count));
            set_i64(parquet_->chunk, 8, current, static_cast<std::int64_t>(folder.flat_logical_size_bytes));
            for (idx_t column = 9; column <= 14; ++column) {
                set_null(parquet_->chunk, column, current);
            }
            set_run_fields(parquet_->chunk, current, config_.run_info);
        }
    }

    flush();
#else
    (void)files;
    (void)folders;
    throw std::runtime_error("metadata parquet output is not available in this build; rebuild with DuckDB");
#endif
}

void MetadataRecordWriter::write_file(const FileSpec& spec) {
    std::string normalized;
    const std::string_view rel_path = normalized_path_view(spec.rel_path, normalized);
    if (config_.format == MetadataRecordFormat::parquet) {
#if HYPERSYNC_HAS_DUCKDB
        check_duckdb_state(duckdb_append_varchar(parquet_->appender, "file"), "duckdb_append_varchar");
        check_duckdb_state(duckdb_append_varchar_length(parquet_->appender,
                                                        rel_path.data(),
                                                        static_cast<idx_t>(rel_path.size())),
                           "duckdb_append_varchar_length");
        check_duckdb_state(duckdb_append_int64(parquet_->appender, static_cast<int64_t>(logical_size(spec))), "duckdb_append_int64");
        check_duckdb_state(duckdb_append_int64(parquet_->appender, static_cast<int64_t>(spec.mtime)), "duckdb_append_int64");
        check_duckdb_state(duckdb_append_int32(parquet_->appender, static_cast<int32_t>(spec.mode)), "duckdb_append_int32");
        check_duckdb_state(duckdb_append_int32(parquet_->appender, static_cast<int32_t>(spec.uid)), "duckdb_append_int32");
        check_duckdb_state(duckdb_append_int32(parquet_->appender, static_cast<int32_t>(spec.gid)), "duckdb_append_int32");
        check_duckdb_state(duckdb_append_null(parquet_->appender), "duckdb_append_null");
        check_duckdb_state(duckdb_append_null(parquet_->appender), "duckdb_append_null");
        check_duckdb_state(duckdb_append_varchar(parquet_->appender, spec.hash_algorithm.c_str()), "duckdb_append_varchar");
        check_duckdb_state(duckdb_append_varchar(parquet_->appender, spec.content_hash.c_str()), "duckdb_append_varchar");
        check_duckdb_state(duckdb_append_int64(parquet_->appender, static_cast<int64_t>(spec.hash_block_size)), "duckdb_append_int64");
        check_duckdb_state(duckdb_append_int64(parquet_->appender, static_cast<int64_t>(spec.hash_block_count)), "duckdb_append_int64");
        check_duckdb_state(duckdb_append_varchar(parquet_->appender, spec.block_hash_algorithm.c_str()), "duckdb_append_varchar");
        check_duckdb_state(duckdb_append_varchar(parquet_->appender, spec.block_hashes.c_str()), "duckdb_append_varchar");
        append_run_fields(parquet_->appender, config_.run_info);
        check_duckdb_state(duckdb_appender_end_row(parquet_->appender), "duckdb_appender_end_row");
        return;
#endif
    }
    if (config_.format == MetadataRecordFormat::csv) {
        output_ << "file,"
                << csv_quote(rel_path) << ','
                << logical_size(spec) << ','
                << spec.mtime << ','
                << spec.mode << ','
                << spec.uid << ','
                << spec.gid << ",,,"
                << csv_quote(spec.hash_algorithm) << ','
                << csv_quote(spec.content_hash) << ','
                << spec.hash_block_size << ','
                << spec.hash_block_count << ','
                << csv_quote(spec.block_hash_algorithm) << ','
                << csv_quote(spec.block_hashes);
        write_csv_run_fields(output_, config_.run_info);
        output_ << '\n';
        return;
    }

    output_ << "file rel_path=" << text_quote(rel_path)
            << " size=" << logical_size(spec)
            << " mtime=" << spec.mtime
            << " mode=" << spec.mode
            << " uid=" << spec.uid
            << " gid=" << spec.gid;
    if (!spec.hash_algorithm.empty() || !spec.content_hash.empty()) {
        output_ << " hash_algorithm=" << text_quote(spec.hash_algorithm)
                << " content_hash=" << text_quote(spec.content_hash);
    }
    if (!spec.block_hash_algorithm.empty() || !spec.block_hashes.empty()) {
        output_ << " hash_block_size=" << spec.hash_block_size
                << " hash_block_count=" << spec.hash_block_count
                << " block_hash_algorithm=" << text_quote(spec.block_hash_algorithm)
                << " block_hashes=" << text_quote(spec.block_hashes);
    }
    if (config_.run_info.has_value()) {
        output_ << " scan_run_id=" << text_quote(config_.run_info->run_id)
                << " run_started_at_utc=" << text_quote(config_.run_info->started_at_utc)
                << " run_started_unix_ns=" << config_.run_info->started_unix_ns
                << " source_root=" << text_quote(config_.run_info->source_root)
                << " run_settings=" << text_quote(config_.run_info->settings_json);
    }
    output_ << '\n';
}

void MetadataRecordWriter::write_folder(const MetadataFolderRecord& folder) {
    std::string normalized;
    const std::string_view rel_path = normalized_path_view(folder.spec.rel_path, normalized);
    if (config_.format == MetadataRecordFormat::parquet) {
#if HYPERSYNC_HAS_DUCKDB
        check_duckdb_state(duckdb_append_varchar(parquet_->appender, "folder"), "duckdb_append_varchar");
        check_duckdb_state(duckdb_append_varchar_length(parquet_->appender,
                                                        rel_path.data(),
                                                        static_cast<idx_t>(rel_path.size())),
                           "duckdb_append_varchar_length");
        check_duckdb_state(duckdb_append_null(parquet_->appender), "duckdb_append_null");
        check_duckdb_state(duckdb_append_int64(parquet_->appender, static_cast<int64_t>(folder.spec.mtime)), "duckdb_append_int64");
        check_duckdb_state(duckdb_append_int32(parquet_->appender, static_cast<int32_t>(folder.spec.mode)), "duckdb_append_int32");
        check_duckdb_state(duckdb_append_int32(parquet_->appender, static_cast<int32_t>(folder.spec.uid)), "duckdb_append_int32");
        check_duckdb_state(duckdb_append_int32(parquet_->appender, static_cast<int32_t>(folder.spec.gid)), "duckdb_append_int32");
        check_duckdb_state(duckdb_append_int64(parquet_->appender, static_cast<int64_t>(folder.flat_file_count)), "duckdb_append_int64");
        check_duckdb_state(duckdb_append_int64(parquet_->appender, static_cast<int64_t>(folder.flat_logical_size_bytes)), "duckdb_append_int64");
        check_duckdb_state(duckdb_append_null(parquet_->appender), "duckdb_append_null");
        check_duckdb_state(duckdb_append_null(parquet_->appender), "duckdb_append_null");
        check_duckdb_state(duckdb_append_null(parquet_->appender), "duckdb_append_null");
        check_duckdb_state(duckdb_append_null(parquet_->appender), "duckdb_append_null");
        check_duckdb_state(duckdb_append_null(parquet_->appender), "duckdb_append_null");
        check_duckdb_state(duckdb_append_null(parquet_->appender), "duckdb_append_null");
        append_run_fields(parquet_->appender, config_.run_info);
        check_duckdb_state(duckdb_appender_end_row(parquet_->appender), "duckdb_appender_end_row");
        return;
#endif
    }
    if (config_.format == MetadataRecordFormat::csv) {
        output_ << "folder,"
                << csv_quote(rel_path) << ','
                << ','
                << folder.spec.mtime << ','
                << folder.spec.mode << ','
                << folder.spec.uid << ','
                << folder.spec.gid << ','
                << folder.flat_file_count << ','
                << folder.flat_logical_size_bytes << ",,,,,,";
        write_csv_run_fields(output_, config_.run_info);
        output_ << '\n';
        return;
    }

    output_ << "folder rel_path=" << text_quote(rel_path)
            << " mtime=" << folder.spec.mtime
            << " mode=" << folder.spec.mode
            << " uid=" << folder.spec.uid
            << " gid=" << folder.spec.gid
            << " flat_file_count=" << folder.flat_file_count
            << " flat_logical_size_bytes=" << folder.flat_logical_size_bytes;
    if (config_.run_info.has_value()) {
        output_ << " scan_run_id=" << text_quote(config_.run_info->run_id)
                << " run_started_at_utc=" << text_quote(config_.run_info->started_at_utc)
                << " run_started_unix_ns=" << config_.run_info->started_unix_ns
                << " source_root=" << text_quote(config_.run_info->source_root)
                << " run_settings=" << text_quote(config_.run_info->settings_json);
    }
    output_ << '\n';
}

}  // namespace hypersync
