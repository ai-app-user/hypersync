#ifndef HYPERSYNC_JOBS_METADATA_STATS_DISCARDER_HPP
#define HYPERSYNC_JOBS_METADATA_STATS_DISCARDER_HPP

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/types.hpp"

namespace hypersync {

class ConfigStore;

struct MetadataStatsDiscarderConfig {
    bool enabled;
    std::uint32_t print_interval_seconds;
    std::string output;
    bool track_unique_folders;

    MetadataStatsDiscarderConfig();
    MetadataStatsDiscarderConfig(bool enabled,
                                 std::uint32_t print_interval_seconds,
                                 std::string output,
                                 bool track_unique_folders = false);
};

struct MetadataStatsSnapshot {
    std::size_t records_discarded = 0;
    std::size_t files_found = 0;
    std::size_t folders_found = 0;
    std::uint64_t logical_size_bytes = 0;
    double elapsed_seconds = 0.0;
    double records_per_second = 0.0;
    double files_per_second = 0.0;
};

[[nodiscard]] MetadataStatsDiscarderConfig load_metadata_stats_discarder_config(const ConfigStore& config);

class MetadataStatsDiscarder {
public:
    explicit MetadataStatsDiscarder(MetadataStatsDiscarderConfig config = {});

    void start();
    void stop();

    void discard_record(const RecBuf& record);
    void record_folder(std::string folder_path);
    void record_batch(std::size_t files_found,
                      std::uint64_t logical_size_bytes,
                      const std::vector<std::string>& folders_found);
    void record_batch(std::size_t files_found,
                      std::uint64_t logical_size_bytes,
                      std::size_t folders_found);
    void maybe_print();
    void print_snapshot() const;

    [[nodiscard]] MetadataStatsSnapshot snapshot() const;
    [[nodiscard]] bool running() const;
    [[nodiscard]] std::size_t accepted_records() const;
    [[nodiscard]] const MetadataStatsDiscarderConfig& config() const;

private:
    [[nodiscard]] std::ostream& output_stream() const;

    mutable std::mutex mutex_;
    MetadataStatsDiscarderConfig config_;
    bool running_ = false;
    std::size_t accepted_ = 0;
    std::size_t discarded_ = 0;
    std::size_t folders_found_ = 0;
    std::uint64_t logical_size_bytes_ = 0;
    std::unordered_set<std::string> folders_;
    std::chrono::steady_clock::time_point started_at_ {};
    std::chrono::steady_clock::time_point last_print_at_ {};
};

}  // namespace hypersync

#endif
