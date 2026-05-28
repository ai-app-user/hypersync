#include "jobs/checker/checker.hpp"

#include "common/config.hpp"

namespace hypersync {

CheckerConfig::CheckerConfig()
    : CheckerConfig(load_checker_config(ConfigStore{})) {}

CheckerConfig::CheckerConfig(bool discard_checked_records,
                             std::size_t worker_count,
                             std::size_t target_request_queue_depth,
                             std::size_t batch_queue_depth)
    : discard_checked_records(discard_checked_records),
      worker_count(worker_count),
      target_request_queue_depth(target_request_queue_depth),
      batch_queue_depth(batch_queue_depth) {}

CheckerConfig load_checker_config(const ConfigStore& config) {
    const ConfigSection values = config.merged_sections(default_job_config_sections("checker"));
    const std::size_t worker_count =
        config_size_t_or(values, "worker_count", config_size_t_or(values, "thread_count", 1));
    const std::size_t target_request_queue_depth =
        config_size_t_or(values,
                         "target_request_queue_depth",
                         config_size_t_or(values, "request_queue_depth", 65536));
    return CheckerConfig(config_bool(values, "discard_checked_records"),
                         worker_count,
                         target_request_queue_depth,
                         config_size_t_or(values, "batch_queue_depth", 65536));
}

Checker::Checker(CheckerConfig config)
    : config_(std::move(config)) {}

void Checker::bind_scans(const ScanIndex* source_scan, const ScanIndex* target_scan) {
    source_scan_ = source_scan;
    target_scan_ = target_scan;
}

bool Checker::should_skip(const RecBuf& record) const {
    if (!record.need_check || target_scan_ == nullptr) {
        return false;
    }

    if (source_scan_ != nullptr) {
        const auto source_snapshot = source_scan_->find(record.rel_path);
        if (source_snapshot.has_value() && target_scan_->file_matches(*source_snapshot)) {
            return true;
        }
    }

    return target_scan_->metadata_matches(record.rel_path, record.size, record.mtime);
}

std::optional<RecBuf> Checker::checked_record(RecBuf record) {
    if (config_.discard_checked_records) {
        ++deferred_records_;
        return std::nullopt;
    }
    return record;
}

const CheckerConfig& Checker::config() const {
    return config_;
}

std::size_t Checker::deferred_records() const {
    return deferred_records_;
}

}  // namespace hypersync
