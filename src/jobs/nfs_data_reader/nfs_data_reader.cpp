#include "jobs/nfs_data_reader/nfs_data_reader.hpp"

#include <algorithm>
#include <cstddef>
#include <string_view>

#include "common/config.hpp"
#include "common/hash_utils.hpp"
#include "core/nfs_backend.hpp"
#include "common/records.hpp"

namespace hypersync {

NfsDataReaderConfig::NfsDataReaderConfig()
    : NfsDataReaderConfig(load_nfs_data_reader_config(ConfigStore{})) {}

NfsDataReaderConfig::NfsDataReaderConfig(std::size_t data_reader_worker_count,
                                         std::size_t outstanding_requests,
                                         std::size_t large_file_parallelism,
                                         std::size_t small_file_threshold,
                                         std::size_t large_chunk_bytes,
                                         double pause_large_pool_percent,
                                         double resume_large_pool_percent,
                                         std::string source_root,
                                         bool copy_data_from_nfs)
    : data_reader_worker_count(data_reader_worker_count),
      outstanding_requests(outstanding_requests),
      large_file_parallelism(large_file_parallelism),
      small_file_threshold(small_file_threshold),
      large_chunk_bytes(large_chunk_bytes),
      pause_large_pool_percent(pause_large_pool_percent),
      resume_large_pool_percent(resume_large_pool_percent),
      source_root(std::move(source_root)),
      copy_data_from_nfs(copy_data_from_nfs) {}

NfsDataReaderConfig load_nfs_data_reader_config(const ConfigStore& config) {
    const ConfigSection values = config.merged_sections(default_job_config_sections("nfs_data_reader"));
    return NfsDataReaderConfig(config_size_t_or(values, "data_reader_worker_count", 2),
                               config_size_t(values, "outstanding_requests"),
                               config_size_t(values, "large_file_parallelism"),
                               config_size_t(values, "small_file_threshold"),
                               config_size_t(values, "large_chunk_bytes"),
                               config_double(values, "pause_large_pool_percent"),
                               config_double(values, "resume_large_pool_percent"),
                               config_string(values, "source_root"),
                               config_bool_or(values, "copy_data_from_nfs", true));
}

NfsDataReader::NfsDataReader(NfsDataReaderConfig config)
    : TypedQueueJob("nfs_data_reader", message_kinds::data_chunk),
      config_(std::move(config)),
      backend_(make_nfs_backend(config_.source_root)) {}

NfsDataReader::~NfsDataReader() = default;

std::vector<DataChunk> NfsDataReader::chunk_file(const FileSpec& file) const {
    const RecBuf record = make_recbuf(file);
    const std::uint64_t data_hash = hash64(file.content);
    const bool is_small = record.size <= config_.small_file_threshold;
    const std::size_t chunk_bytes = is_small ? config_.small_file_threshold : config_.large_chunk_bytes;

    std::vector<DataChunk> chunks;
    std::size_t chunk_index = 0;
    for (std::size_t offset = 0; offset < file.content.size() || (file.content.empty() && offset == 0); offset += chunk_bytes) {
        const std::size_t remaining = file.content.size() > offset ? file.content.size() - offset : 0;
        const std::size_t len = file.content.empty() ? 0 : std::min<std::size_t>(chunk_bytes, remaining);

        DataChunk chunk;
        chunk.entry_id = chunk_index++;
        chunk.data = file.content.substr(offset, len);
        chunk.trailer.file_id = record.own_hash;
        chunk.trailer.folder_hash = record.folder_hash;
        chunk.trailer.data_offset = offset;
        chunk.trailer.data_len = len;
        chunk.trailer.file_size = record.size;
        chunk.trailer.data_hash = data_hash;
        chunk.trailer.mtime = record.mtime;
        chunk.trailer.mode = record.mode;
        chunk.trailer.uid = record.uid;
        chunk.trailer.gid = record.gid;
        chunk.trailer.chunk_hash = chunk_hash32(std::string_view(chunk.data));
        chunk.trailer.rel_path = record.rel_path;
        chunk.trailer.flags = is_small ? kFlagSmallFile : 0U;
        if (offset + len >= file.content.size()) {
            chunk.trailer.flags |= kFlagLastChunk | kFlagHashValid;
        }
        chunks.push_back(std::move(chunk));
    }

    return chunks;
}

FileSpec NfsDataReader::load_file(std::string_view rel_path) const {
    return backend().load_file(rel_path, config_.outstanding_requests);
}

std::uint64_t NfsDataReader::read_file_bytes(const FileSpec& file) const {
    return read_file_bytes(file, {});
}

std::uint64_t NfsDataReader::read_file_bytes(
    const FileSpec& file,
    const std::function<void(std::uint64_t)>& bytes_visitor) const {
    return backend().read_file_discard(file.rel_path, file.declared_size, config_.outstanding_requests, bytes_visitor);
}

std::uint64_t NfsDataReader::stream_file_data(
    const FileSpec& file,
    const std::function<void(std::string_view)>& data_visitor) const {
    return backend().read_file_stream(file.rel_path, file.declared_size, config_.outstanding_requests, data_visitor);
}

std::uint64_t NfsDataReader::stream_file_owned_chunks(
    const FileSpec& file,
    const std::function<void(OwnedFileChunk&&)>& data_visitor) const {
    return backend().read_file_owned_chunks(file.rel_path, file.declared_size, config_.outstanding_requests, data_visitor);
}

std::uint64_t NfsDataReader::stream_file_pooled_chunks(
    const FileSpec& file,
    DataSlotPool& pool,
    const std::function<void(PooledFileChunk&&)>& data_visitor) const {
    return backend().read_file_pooled_chunks(file.rel_path,
                                            file.declared_size,
                                            config_.outstanding_requests,
                                            pool,
                                            data_visitor);
}

std::uint64_t NfsDataReader::stream_file_raw_chunks(
    const FileSpec& file,
    RawBufferPool& pool,
    const std::function<void(RawFileChunk&&)>& data_visitor,
    const std::function<bool()>& should_stop) const {
    return backend().read_file_raw_chunks(file.rel_path,
                                         file.declared_size,
                                         config_.outstanding_requests,
                                         pool,
                                         data_visitor,
                                         should_stop,
                                         config_.copy_data_from_nfs);
}

std::uint64_t NfsDataReader::visit_file_chunks(
    const FileSpec& file,
    const std::function<void(std::uint64_t offset, std::string_view data)>& data_visitor) const {
    return backend().visit_file_chunks(file.rel_path, file.declared_size, config_.outstanding_requests, data_visitor);
}

std::vector<DataChunk> NfsDataReader::read_file(std::string_view rel_path) const {
    return chunk_file(load_file(rel_path));
}

void NfsDataReader::publish_file(const FileSpec& file) {
    for (auto& chunk : chunk_file(file)) {
        publish_item(std::move(chunk));
    }
}

void NfsDataReader::publish_path(std::string_view rel_path) {
    for (auto& chunk : read_file(rel_path)) {
        publish_item(std::move(chunk));
    }
}

bool NfsDataReader::should_pause(double large_pool_usage_percent) const {
    return large_pool_usage_percent >= config_.pause_large_pool_percent;
}

bool NfsDataReader::should_resume(double large_pool_usage_percent) const {
    return large_pool_usage_percent <= config_.resume_large_pool_percent;
}

bool NfsDataReader::using_async_backend() const {
    return backend().uses_async_api();
}

const NfsDataReaderConfig& NfsDataReader::config() const {
    return config_;
}

NfsBackend& NfsDataReader::backend() const {
    return *backend_;
}

}  // namespace hypersync
