#ifndef HYPERSYNC_JOBS_SCAN_WRITER_HPP
#define HYPERSYNC_JOBS_SCAN_WRITER_HPP

#include <cstddef>
#include <optional>

#include "common/types.hpp"
#include "jobs/queue_job.hpp"

namespace hypersync {

class ConfigStore;

struct ScanWriterConfig {
    char scan_side;
    bool flush_per_file;

    ScanWriterConfig();
    ScanWriterConfig(char scan_side, bool flush_per_file);
};

[[nodiscard]] ScanWriterConfig load_scan_writer_config(const ConfigStore& config);

class ScanWriter : public TypedQueueJob<FileSnapshot> {
public:
    explicit ScanWriter(ScanWriterConfig config = {});

    [[nodiscard]] std::optional<FileSnapshot> snapshot_from_chunk(const DataChunk& chunk) const;
    void record_chunk(const DataChunk& chunk);
    [[nodiscard]] std::size_t rows_written() const;
    [[nodiscard]] const ScanWriterConfig& config() const;

private:
    ScanWriterConfig config_;
    std::size_t rows_written_ = 0;
};

}  // namespace hypersync

#endif
