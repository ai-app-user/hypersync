#include "jobs/data_writer/data_writer.hpp"

#include "common/config.hpp"
#include "common/path_utils.hpp"

namespace hypersync {

DataWriterConfig::DataWriterConfig()
    : DataWriterConfig(load_data_writer_config(ConfigStore{})) {}

DataWriterConfig::DataWriterConfig(std::size_t handle_cache_limit, bool verify_hash)
    : handle_cache_limit(handle_cache_limit),
      verify_hash(verify_hash) {}

DataWriterConfig load_data_writer_config(const ConfigStore& config) {
    const ConfigSection values = config.merged_sections(default_job_config_sections("data_writer"));
    return DataWriterConfig(config_size_t(values, "handle_cache_limit"),
                            config_bool(values, "verify_hash"));
}

DataWriter::DataWriter(DataWriterConfig config)
    : TypedQueueJob("data_writer", message_kinds::data_chunk), config_(std::move(config)) {}

void DataWriter::predeclare_directory(std::string path) {
    dir_cache_.insert(normalize_path(path));
}

bool DataWriter::known_directory(std::string_view path) const {
    return dir_cache_.find(normalize_path(path)) != dir_cache_.end();
}

void DataWriter::queue_chunk(DataChunk chunk) {
    const std::string dir = parent_path(chunk.trailer.rel_path);
    if (!known_directory(dir)) {
        predeclare_directory(dir);
    }

    auto& progress = progress_[chunk.trailer.file_id];
    ++progress.completed_chunks;
    if ((chunk.trailer.flags & kFlagLastChunk) != 0U && progress.total_chunks < progress.completed_chunks) {
        progress.total_chunks = progress.completed_chunks;
    }
    if (progress.total_chunks != 0 && progress.total_chunks == progress.completed_chunks &&
        completed_file_ids_.insert(chunk.trailer.file_id).second) {
        ++completed_files_;
    }

    publish_item(std::move(chunk));
}

ChunkProgress DataWriter::progress_for(std::uint64_t file_id) const {
    const auto it = progress_.find(file_id);
    if (it == progress_.end()) {
        return {};
    }
    return it->second;
}

std::size_t DataWriter::completed_files() const {
    return completed_files_;
}

const DataWriterConfig& DataWriter::config() const {
    return config_;
}

}  // namespace hypersync
