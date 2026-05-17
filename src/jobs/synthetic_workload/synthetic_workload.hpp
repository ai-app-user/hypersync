#ifndef HYPERSYNC_JOBS_SYNTHETIC_WORKLOAD_HPP
#define HYPERSYNC_JOBS_SYNTHETIC_WORKLOAD_HPP

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

namespace hypersync {

inline constexpr std::uint64_t kSyntheticUnboundedSize =
    std::numeric_limits<std::uint64_t>::max();
inline constexpr std::size_t kSyntheticSizeBucketCount = 10;
inline constexpr std::size_t kSyntheticFolderFanoutBucketCount = 8;
inline constexpr std::size_t kSyntheticFolderDepthBucketCount = 10;
inline constexpr std::size_t kSyntheticEntriesPerPageBucketCount = 8;
inline constexpr std::size_t kSyntheticLatencyBucketCount = 8;
inline constexpr std::size_t kSyntheticHandleBytes = 64;
inline constexpr std::size_t kSyntheticPathBytes = 256;

struct SyntheticLatencyPercentiles {
    std::uint64_t p50_us = 0;
    std::uint64_t p90_us = 0;
    std::uint64_t p99_us = 0;
    std::uint64_t max_us = 0;
};

struct SyntheticPhaseProfile {
    std::string name;
    std::uint64_t file_count = 0;
    std::uint64_t folder_count = 0;
    std::uint64_t logical_size_bytes = 0;
    std::uint64_t small_file_count = 0;
    std::uint64_t large_file_count = 0;
    std::array<std::uint64_t, kSyntheticSizeBucketCount> size_file_counts {};
    std::array<std::uint64_t, kSyntheticSizeBucketCount> size_logical_bytes {};
    std::array<std::uint64_t, kSyntheticFolderFanoutBucketCount> folder_fanout_counts {};
    std::array<std::uint64_t, kSyntheticFolderFanoutBucketCount> files_per_folder_counts {};
    std::array<std::uint64_t, kSyntheticFolderFanoutBucketCount> subdirs_per_folder_counts {};
    std::array<std::uint64_t, kSyntheticFolderDepthBucketCount> folder_depth_counts {};
    std::array<std::uint64_t, kSyntheticEntriesPerPageBucketCount> entries_per_page_counts {};
    std::array<std::uint64_t, kSyntheticLatencyBucketCount> readdirplus_page_latency_counts {};
    std::array<std::uint64_t, kSyntheticLatencyBucketCount> readdirplus_decode_latency_counts {};
    std::uint64_t filename_length_sum = 0;
    std::uint64_t depth_sum = 0;
    std::uint64_t empty_folder_count = 0;
    std::uint64_t near_empty_folder_count = 0;
    std::uint64_t directory_count = 0;
    std::uint64_t max_depth = 0;
    std::uint64_t readdirplus_page_count = 0;
    std::uint64_t readdirplus_page_entries = 0;
    std::uint64_t readdirplus_page_requested_bytes = 0;
    std::uint64_t readdirplus_page_latency_sum_us = 0;
    std::uint64_t readdirplus_decode_latency_sum_us = 0;
    SyntheticLatencyPercentiles readdirplus_page_latency;
    SyntheticLatencyPercentiles readdirplus_decode_latency;
    SyntheticLatencyPercentiles small_read_latency;
    SyntheticLatencyPercentiles large_read_latency;
};

struct SyntheticWorkloadProfile {
    std::uint32_t version = 1;
    std::uint64_t seed = 0x9e3779b97f4a7c15ULL;
    std::uint64_t small_file_threshold_bytes = 128U * 1024U;
    std::vector<SyntheticPhaseProfile> phases;
};

struct SyntheticProfileCaptureConfig {
    std::uint64_t block_file_count = 1'000'000;
    double small_ratio_shift_threshold = 0.05;
    std::uint64_t small_file_threshold_bytes = 128U * 1024U;
    std::uint64_t seed = 0x9e3779b97f4a7c15ULL;
};

struct SyntheticObservedFile {
    std::uint64_t size_bytes = 0;
    std::uint32_t uid = 0;
    std::uint32_t gid = 0;
    std::uint32_t mode = 0644;
    std::uint16_t filename_length = 0;
    std::uint16_t depth = 0;
    std::uint32_t files_in_folder = 0;
};

class SyntheticProfileBuilder {
public:
    struct Accumulator {
        std::uint64_t file_count = 0;
        std::uint64_t folder_count = 0;
        std::uint64_t logical_size_bytes = 0;
        std::uint64_t small_file_count = 0;
        std::uint64_t large_file_count = 0;
        std::array<std::uint64_t, kSyntheticSizeBucketCount> size_file_counts {};
        std::array<std::uint64_t, kSyntheticSizeBucketCount> size_logical_bytes {};
        std::array<std::uint64_t, kSyntheticFolderFanoutBucketCount> folder_fanout_counts {};
        std::array<std::uint64_t, kSyntheticFolderFanoutBucketCount> files_per_folder_counts {};
        std::array<std::uint64_t, kSyntheticFolderFanoutBucketCount> subdirs_per_folder_counts {};
        std::array<std::uint64_t, kSyntheticFolderDepthBucketCount> folder_depth_counts {};
        std::array<std::uint64_t, kSyntheticEntriesPerPageBucketCount> entries_per_page_counts {};
        std::array<std::uint64_t, kSyntheticLatencyBucketCount> readdirplus_page_latency_counts {};
        std::array<std::uint64_t, kSyntheticLatencyBucketCount> readdirplus_decode_latency_counts {};
        std::uint64_t filename_length_sum = 0;
        std::uint64_t depth_sum = 0;
        std::uint64_t empty_folder_count = 0;
        std::uint64_t near_empty_folder_count = 0;
        std::uint64_t directory_count = 0;
        std::uint64_t max_depth = 0;
        std::uint64_t readdirplus_page_count = 0;
        std::uint64_t readdirplus_page_entries = 0;
        std::uint64_t readdirplus_page_requested_bytes = 0;
        std::uint64_t readdirplus_page_latency_sum_us = 0;
        std::uint64_t readdirplus_decode_latency_sum_us = 0;
    };

    explicit SyntheticProfileBuilder(SyntheticProfileCaptureConfig config = {});

    void observe_file(const SyntheticObservedFile& file);
    [[nodiscard]] SyntheticWorkloadProfile finish();
    [[nodiscard]] const SyntheticProfileCaptureConfig& config() const noexcept;

private:
    void seal_active_phase();
    void maybe_finish_block();
    [[nodiscard]] SyntheticPhaseProfile make_phase(const Accumulator& accumulator,
                                                   std::size_t index) const;

    SyntheticProfileCaptureConfig config_;
    SyntheticWorkloadProfile profile_;
    Accumulator active_;
    Accumulator block_;
};

struct SyntheticReplayConfig {
    SyntheticWorkloadProfile profile;
    double file_count_scale = 1.0;
    double data_size_scale = 1.0;
    bool latency_enabled = false;
};

enum class SyntheticPayloadPattern {
    zero,
    repeated,
    deterministic,
};

struct SyntheticPayloadView {
    const std::uint8_t* data = nullptr;
    std::size_t size = 0;
    std::size_t capacity = 0;
};

struct SyntheticFileView {
    std::array<char, kSyntheticPathBytes> path {};
    std::size_t path_length = 0;
    std::array<std::uint8_t, kSyntheticHandleBytes> nfs_handle {};
    std::uint64_t file_id = 0;
    std::uint64_t folder_id = 0;
    std::uint64_t size_bytes = 0;
    std::uint64_t latency_us = 0;
    bool small = true;

    [[nodiscard]] std::string_view path_view() const noexcept {
        return std::string_view(path.data(), path_length);
    }
};

class SyntheticReplayCursor {
public:
    explicit SyntheticReplayCursor(SyntheticReplayConfig config);

    [[nodiscard]] bool next_file(SyntheticFileView& out) noexcept;
    [[nodiscard]] std::uint64_t files_emitted() const noexcept;
    [[nodiscard]] std::uint64_t bytes_emitted() const noexcept;

private:
    [[nodiscard]] const SyntheticPhaseProfile* current_phase() const noexcept;
    [[nodiscard]] std::uint64_t scaled_phase_files(const SyntheticPhaseProfile& phase) const noexcept;
    [[nodiscard]] std::uint64_t sample_size(const SyntheticPhaseProfile& phase,
                                            std::uint64_t file_id) const noexcept;
    [[nodiscard]] std::uint64_t sample_latency(const SyntheticPhaseProfile& phase,
                                               bool small,
                                               std::uint64_t file_id) const noexcept;
    void fill_path(SyntheticFileView& out) const noexcept;
    void fill_handle(SyntheticFileView& out) const noexcept;

    SyntheticReplayConfig config_;
    std::size_t phase_index_ = 0;
    std::uint64_t phase_file_offset_ = 0;
    std::uint64_t files_emitted_ = 0;
    std::uint64_t bytes_emitted_ = 0;
};

class SyntheticPayloadPool {
public:
    SyntheticPayloadPool(std::size_t small_block_bytes = 4U * 1024U,
                         std::size_t large_block_bytes = 1024U * 1024U,
                         SyntheticPayloadPattern pattern = SyntheticPayloadPattern::deterministic,
                         std::uint64_t seed = 0x9e3779b97f4a7c15ULL);

    [[nodiscard]] SyntheticPayloadView payload_for(const SyntheticFileView& file) const noexcept;
    [[nodiscard]] SyntheticPayloadView small_payload(std::size_t bytes) const noexcept;
    [[nodiscard]] SyntheticPayloadView large_payload(std::size_t bytes) const noexcept;
    [[nodiscard]] std::size_t small_block_bytes() const noexcept;
    [[nodiscard]] std::size_t large_block_bytes() const noexcept;

private:
    void fill(std::vector<std::uint8_t>& target,
              SyntheticPayloadPattern pattern,
              std::uint64_t seed);

    std::vector<std::uint8_t> small_;
    std::vector<std::uint8_t> large_;
};

[[nodiscard]] std::array<std::uint64_t, kSyntheticSizeBucketCount>
synthetic_size_bucket_bounds() noexcept;
[[nodiscard]] std::array<std::uint64_t, kSyntheticLatencyBucketCount>
synthetic_latency_bucket_bounds_us() noexcept;
[[nodiscard]] std::size_t synthetic_size_bucket_index(std::uint64_t size_bytes) noexcept;
[[nodiscard]] std::size_t synthetic_folder_fanout_bucket_index(std::uint64_t files_in_folder) noexcept;
[[nodiscard]] std::size_t synthetic_folder_depth_bucket_index(std::uint64_t depth) noexcept;
[[nodiscard]] std::size_t synthetic_entries_per_page_bucket_index(std::uint64_t entries) noexcept;
[[nodiscard]] std::size_t synthetic_latency_bucket_index_us(std::uint64_t latency_us) noexcept;
[[nodiscard]] std::uint64_t synthetic_splitmix64(std::uint64_t value) noexcept;

}  // namespace hypersync

#endif
