#include "jobs/checker/checker.hpp"

#include "common/config.hpp"

namespace hypersync {

CheckerConfig::CheckerConfig()
    : CheckerConfig(load_checker_config(ConfigStore{})) {}

CheckerConfig::CheckerConfig(bool discard_checked_records)
    : discard_checked_records(discard_checked_records) {}

CheckerConfig load_checker_config(const ConfigStore& config) {
    const ConfigSection values = config.merged_sections(default_job_config_sections("checker"));
    return CheckerConfig(config_bool(values, "discard_checked_records"));
}

Checker::Checker(CheckerConfig config)
    : TypedQueueJob("checker", message_kinds::file_record), config_(std::move(config)) {}

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

void Checker::queue_checked_record(RecBuf record) {
    if (config_.discard_checked_records) {
        record_deferred();
        return;
    }
    publish_item(std::move(record));
}

const CheckerConfig& Checker::config() const {
    return config_;
}

}  // namespace hypersync
