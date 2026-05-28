#include "jobs/metadata_stats_discarder/metadata_stats_discarder.hpp"

#include <iostream>
#include <stdexcept>
#include <utility>

#include "common/config.hpp"
#include "common/path_utils.hpp"

namespace hypersync {

MetadataStatsDiscarderConfig::MetadataStatsDiscarderConfig()
    : MetadataStatsDiscarderConfig(load_metadata_stats_discarder_config(ConfigStore{})) {}

MetadataStatsDiscarderConfig::MetadataStatsDiscarderConfig(bool enabled,
                                                           std::uint32_t print_interval_seconds,
                                                           std::string output,
                                                           bool track_unique_folders)
    : enabled(enabled),
      print_interval_seconds(print_interval_seconds),
      output(std::move(output)),
      track_unique_folders(track_unique_folders) {}

MetadataStatsDiscarderConfig load_metadata_stats_discarder_config(const ConfigStore& config) {
    const ConfigSection values = config.merged_sections(default_job_config_sections("metadata_stats_discarder"));
    return MetadataStatsDiscarderConfig(config_bool_or(values, "enabled", false),
                                        config_u32_or(values, "print_interval_seconds", 5),
                                        config_string_or(values, "output", "stderr"),
                                        config_bool_or(values, "track_unique_folders", false));
}

MetadataStatsDiscarder::MetadataStatsDiscarder(MetadataStatsDiscarderConfig config)
    : config_(std::move(config)) {
    if (config_.print_interval_seconds == 0) {
        throw std::invalid_argument("metadata stats print interval must be positive");
    }
    if (config_.output != "stderr" && config_.output != "stdout") {
        throw std::invalid_argument("metadata stats output must be stderr or stdout");
    }
}

void MetadataStatsDiscarder::start() {
    std::lock_guard<std::mutex> lock(mutex_);
    running_ = true;
    started_at_ = std::chrono::steady_clock::now();
    last_print_at_ = started_at_;
}

void MetadataStatsDiscarder::stop() {
    std::lock_guard<std::mutex> lock(mutex_);
    running_ = false;
}

void MetadataStatsDiscarder::discard_record(const RecBuf& record) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++accepted_;
        ++discarded_;
        logical_size_bytes_ += record.size;
        if (config_.track_unique_folders) {
            folders_.insert(parent_path(record.rel_path));
        } else {
            ++folders_found_;
        }
    }
    maybe_print();
}

void MetadataStatsDiscarder::record_folder(std::string folder_path) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++accepted_;
        if (config_.track_unique_folders) {
            folders_.insert(normalize_path(folder_path));
        } else {
            ++folders_found_;
        }
    }
    maybe_print();
}

void MetadataStatsDiscarder::record_batch(std::size_t files_found,
                                          std::uint64_t logical_size_bytes,
                                          const std::vector<std::string>& folders_found) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        accepted_ += files_found + folders_found.size();
        discarded_ += files_found;
        logical_size_bytes_ += logical_size_bytes;
        if (config_.track_unique_folders) {
            for (const auto& folder : folders_found) {
                folders_.insert(normalize_path(folder));
            }
        } else {
            folders_found_ += folders_found.size();
        }
    }
    maybe_print();
}

void MetadataStatsDiscarder::record_batch(std::size_t files_found,
                                          std::uint64_t logical_size_bytes,
                                          std::size_t folders_found) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        accepted_ += files_found + folders_found;
        discarded_ += files_found;
        logical_size_bytes_ += logical_size_bytes;
        folders_found_ += folders_found;
    }
    maybe_print();
}

void MetadataStatsDiscarder::maybe_print() {
    bool should_print = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_) {
            return;
        }
        const auto now = std::chrono::steady_clock::now();
        const auto interval = std::chrono::seconds(config_.print_interval_seconds);
        if (now - last_print_at_ >= interval) {
            last_print_at_ = now;
            should_print = true;
        }
    }

    if (should_print) {
        print_snapshot();
    }
}

void MetadataStatsDiscarder::print_snapshot() const {
    const MetadataStatsSnapshot current = snapshot();
    output_stream() << "metadata_stats records_per_second=" << current.records_per_second
                    << " files_per_second=" << current.files_per_second
                    << " files_found=" << current.files_found
                    << " folders_found=" << current.folders_found
                    << " logical_size_bytes=" << current.logical_size_bytes
                    << " elapsed_seconds=" << current.elapsed_seconds << '\n';
}

MetadataStatsSnapshot MetadataStatsDiscarder::snapshot() const {
    const auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mutex_);
    const double elapsed = started_at_ == std::chrono::steady_clock::time_point{}
                               ? 0.0
                               : std::chrono::duration<double>(now - started_at_).count();

    MetadataStatsSnapshot result;
    result.records_discarded = discarded_;
    result.files_found = discarded_;
    result.folders_found = config_.track_unique_folders ? folders_.size() : folders_found_;
    result.logical_size_bytes = logical_size_bytes_;
    result.elapsed_seconds = elapsed;
    result.records_per_second = elapsed > 0.0 ? static_cast<double>(accepted_) / elapsed : 0.0;
    result.files_per_second = elapsed > 0.0 ? static_cast<double>(result.files_found) / elapsed : 0.0;
    return result;
}

bool MetadataStatsDiscarder::running() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return running_;
}

std::size_t MetadataStatsDiscarder::accepted_records() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return accepted_;
}

const MetadataStatsDiscarderConfig& MetadataStatsDiscarder::config() const {
    return config_;
}

std::ostream& MetadataStatsDiscarder::output_stream() const {
    return config_.output == "stdout" ? std::cout : std::cerr;
}

}  // namespace hypersync
