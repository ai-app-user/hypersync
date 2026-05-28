#ifndef HYPERSYNC_JOBS_CHECKER_HPP
#define HYPERSYNC_JOBS_CHECKER_HPP

#include <optional>

#include "common/scan_index.hpp"
#include "common/types.hpp"

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

class Checker {
public:
    explicit Checker(CheckerConfig config = {});

    void bind_scans(const ScanIndex* source_scan, const ScanIndex* target_scan);
    [[nodiscard]] bool should_skip(const RecBuf& record) const;
    [[nodiscard]] std::optional<RecBuf> checked_record(RecBuf record);

    [[nodiscard]] const CheckerConfig& config() const;
    [[nodiscard]] std::size_t deferred_records() const;

private:
    CheckerConfig config_;
    const ScanIndex* source_scan_ = nullptr;
    const ScanIndex* target_scan_ = nullptr;
    std::size_t deferred_records_ = 0;
};

}  // namespace hypersync

#endif
