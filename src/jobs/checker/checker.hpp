#ifndef HYPERSYNC_JOBS_CHECKER_HPP
#define HYPERSYNC_JOBS_CHECKER_HPP

#include "common/scan_index.hpp"
#include "common/types.hpp"
#include "jobs/queue_job.hpp"

#include <cstddef>

namespace hypersync {

class ConfigStore;

struct CheckerConfig {
    bool discard_checked_records = false;
    std::size_t worker_count = 1;
    std::size_t target_request_queue_depth = 65536;
    std::size_t batch_queue_depth = 65536;

    CheckerConfig();
    explicit CheckerConfig(bool discard_checked_records,
                           std::size_t worker_count = 1,
                           std::size_t target_request_queue_depth = 65536,
                           std::size_t batch_queue_depth = 65536);
};

[[nodiscard]] CheckerConfig load_checker_config(const ConfigStore& config);

class Checker : public TypedQueueJob<RecBuf> {
public:
    explicit Checker(CheckerConfig config = {});

    void bind_scans(const ScanIndex* source_scan, const ScanIndex* target_scan);
    [[nodiscard]] bool should_skip(const RecBuf& record) const;
    void queue_checked_record(RecBuf record);

    [[nodiscard]] const CheckerConfig& config() const;

private:
    CheckerConfig config_;
    const ScanIndex* source_scan_ = nullptr;
    const ScanIndex* target_scan_ = nullptr;
};

}  // namespace hypersync

#endif
