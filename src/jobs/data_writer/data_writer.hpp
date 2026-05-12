#ifndef HYPERSYNC_JOBS_DATA_WRITER_HPP
#define HYPERSYNC_JOBS_DATA_WRITER_HPP

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include "common/types.hpp"
#include "jobs/queue_job.hpp"

namespace hypersync {

class ConfigStore;

struct DataWriterConfig {
    std::size_t handle_cache_limit;
    bool verify_hash;

    DataWriterConfig();
    DataWriterConfig(std::size_t handle_cache_limit, bool verify_hash);
};

[[nodiscard]] DataWriterConfig load_data_writer_config(const ConfigStore& config);

class DataWriter : public TypedQueueJob<DataChunk> {
public:
    explicit DataWriter(DataWriterConfig config = {});

    void predeclare_directory(std::string path);
    [[nodiscard]] bool known_directory(std::string_view path) const;
    void queue_chunk(DataChunk chunk);
    [[nodiscard]] ChunkProgress progress_for(std::uint64_t file_id) const;
    [[nodiscard]] std::size_t completed_files() const;
    [[nodiscard]] const DataWriterConfig& config() const;

private:
    DataWriterConfig config_;
    std::unordered_set<std::string> dir_cache_;
    std::unordered_set<std::uint64_t> completed_file_ids_;
    std::unordered_map<std::uint64_t, ChunkProgress> progress_;
    std::size_t completed_files_ = 0;
};

}  // namespace hypersync

#endif
