#ifndef HYPERSYNC_JOBS_DATA_CACHER_HPP
#define HYPERSYNC_JOBS_DATA_CACHER_HPP

#include <cstdint>
#include <filesystem>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/slot_pool.hpp"
#include "common/types.hpp"
#include "jobs/queue_job.hpp"

namespace hypersync {

class ConfigStore;

struct DataCacherConfig {
    EndpointRole role;
    double spill_threshold_percent;
    double drain_threshold_percent;
    std::string cache_path;
    std::size_t drive_count;
    std::size_t queue_depth_per_drive;

    DataCacherConfig();
    DataCacherConfig(EndpointRole role,
                     double spill_threshold_percent,
                     double drain_threshold_percent,
                     std::string cache_path,
                     std::size_t drive_count,
                     std::size_t queue_depth_per_drive);
};

[[nodiscard]] DataCacherConfig load_data_cacher_config(const ConfigStore& config);

class DataCacher : public TypedQueueJob<DataChunk> {
public:
    explicit DataCacher(DataCacherConfig config = {});

    void cache_chunk(DataChunk chunk);
    void cache_slot(const DataSlotPool& pool, const DataSlotHandle& handle);
    [[nodiscard]] bool should_spill(double ram_usage_percent) const;
    [[nodiscard]] bool should_drain(double ram_usage_percent) const;
    [[nodiscard]] ChunkProgress manifest_for(std::uint64_t file_id) const;
    [[nodiscard]] std::filesystem::path cache_path_for(std::uint64_t entry_id) const;
    [[nodiscard]] std::vector<std::uint64_t> cached_entries_for(std::uint64_t file_id) const;
    void set_cached_file_hash(std::uint64_t file_id, std::uint64_t data_hash);
    [[nodiscard]] DataChunk take_chunk(std::uint64_t entry_id);
    [[nodiscard]] DataSlotHandle take_slot(std::uint64_t entry_id, DataSlotPool& pool);
    [[nodiscard]] std::uint64_t cached_bytes() const;
    [[nodiscard]] double cache_usage_percent(std::uint64_t capacity_bytes) const;
    [[nodiscard]] const DataCacherConfig& config() const;

private:
    struct CacheEntry {
        std::filesystem::path path;
        std::uint64_t file_id = 0;
        std::uint64_t bytes_on_disk = 0;
    };

    mutable std::mutex mutex_;
    DataCacherConfig config_;
    std::unordered_map<std::uint64_t, ChunkProgress> manifests_;
    std::unordered_map<std::uint64_t, std::vector<std::uint64_t>> entries_by_file_;
    std::unordered_map<std::uint64_t, CacheEntry> entries_;
    std::uint64_t next_entry_id_ = 1;
    std::uint64_t cached_bytes_ = 0;
};

}  // namespace hypersync

#endif
