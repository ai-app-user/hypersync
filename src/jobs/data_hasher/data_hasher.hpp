#ifndef HYPERSYNC_JOBS_DATA_HASHER_HPP
#define HYPERSYNC_JOBS_DATA_HASHER_HPP

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>

#include "common/buffer_pool.hpp"
#include "common/content_hash.hpp"
#include "jobs/buffer_transform_job.hpp"

namespace hypersync {

class ConfigStore;

struct DataHasherConfig {
    std::size_t worker_count = 1;
    ContentHashAlgorithm algorithm = ContentHashAlgorithm::xxh64;
    // Repeats the selected hash per buffer for CPU-burn pipeline proofs. The
    // job still reports each payload byte once so reader throughput is stable.
    std::size_t work_factor = 1;

    DataHasherConfig();
    DataHasherConfig(std::size_t worker_count,
                     ContentHashAlgorithm algorithm,
                     std::size_t work_factor = 1);
};

struct DataHasherStats {
    bool running = false;
    std::size_t worker_count = 0;
    std::string algorithm;
    std::size_t work_factor = 1;
    std::uint64_t buffers_hashed = 0;
    std::uint64_t bytes_hashed = 0;
    std::uint64_t digest_marker = 0;
};

[[nodiscard]] DataHasherConfig load_data_hasher_config(const ConfigStore& config);

class DataHasherJob : public BufferTransformJob {
public:
    DataHasherJob(DataHasherConfig config,
                  BufQueue& input,
                  BufQueue& output,
                  const BufferPoolRegistry& registry);
    ~DataHasherJob() override;

    [[nodiscard]] DataHasherStats stats() const;

private:
    std::uint64_t process_buffer(const BufferHandle& handle, RawBufferPool& pool) override;

    DataHasherConfig config_;
    std::atomic<std::uint64_t> digest_marker_ {0};
};

}  // namespace hypersync

#endif
