#include "jobs/synthetic_workload/synthetic_workload.hpp"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <utility>

namespace hypersync {
namespace {

char* append_literal(char* out, std::string_view value) noexcept {
    std::memcpy(out, value.data(), value.size());
    return out + value.size();
}

char* append_fixed_u64(char* out, std::uint64_t value, std::size_t width) noexcept {
    for (std::size_t offset = 0; offset < width; ++offset) {
        const std::size_t digit_index = width - 1U - offset;
        out[digit_index] = static_cast<char>('0' + (value % 10U));
        value /= 10U;
    }
    return out + width;
}

void add_file_to_accumulator(SyntheticProfileBuilder::Accumulator& accumulator,
                             const SyntheticObservedFile& file,
                             std::uint64_t small_threshold) {
    ++accumulator.file_count;
    accumulator.logical_size_bytes += file.size_bytes;
    if (file.size_bytes <= small_threshold) {
        ++accumulator.small_file_count;
    } else {
        ++accumulator.large_file_count;
    }
    const std::size_t size_bucket = synthetic_size_bucket_index(file.size_bytes);
    ++accumulator.size_file_counts[size_bucket];
    accumulator.size_logical_bytes[size_bucket] += file.size_bytes;
    ++accumulator.folder_fanout_counts[synthetic_folder_fanout_bucket_index(file.files_in_folder)];
    accumulator.filename_length_sum += file.filename_length;
    accumulator.depth_sum += file.depth;
}

void subtract_accumulator(SyntheticProfileBuilder::Accumulator& target,
                          const SyntheticProfileBuilder::Accumulator& value) {
    target.file_count -= std::min(target.file_count, value.file_count);
    target.folder_count -= std::min(target.folder_count, value.folder_count);
    target.logical_size_bytes -= std::min(target.logical_size_bytes, value.logical_size_bytes);
    target.small_file_count -= std::min(target.small_file_count, value.small_file_count);
    target.large_file_count -= std::min(target.large_file_count, value.large_file_count);
    target.filename_length_sum -= std::min(target.filename_length_sum, value.filename_length_sum);
    target.depth_sum -= std::min(target.depth_sum, value.depth_sum);
    target.empty_folder_count -= std::min(target.empty_folder_count, value.empty_folder_count);
    target.near_empty_folder_count -= std::min(target.near_empty_folder_count, value.near_empty_folder_count);
    target.directory_count -= std::min(target.directory_count, value.directory_count);
    target.max_depth = std::max(target.max_depth, value.max_depth);
    target.readdirplus_page_count -= std::min(target.readdirplus_page_count, value.readdirplus_page_count);
    target.readdirplus_page_entries -= std::min(target.readdirplus_page_entries, value.readdirplus_page_entries);
    target.readdirplus_page_requested_bytes -=
        std::min(target.readdirplus_page_requested_bytes, value.readdirplus_page_requested_bytes);
    target.readdirplus_page_latency_sum_us -=
        std::min(target.readdirplus_page_latency_sum_us, value.readdirplus_page_latency_sum_us);
    target.readdirplus_decode_latency_sum_us -=
        std::min(target.readdirplus_decode_latency_sum_us, value.readdirplus_decode_latency_sum_us);
    target.sampled_small_read_files -= std::min(target.sampled_small_read_files, value.sampled_small_read_files);
    target.sampled_large_read_files -= std::min(target.sampled_large_read_files, value.sampled_large_read_files);
    target.sampled_small_read_bytes -= std::min(target.sampled_small_read_bytes, value.sampled_small_read_bytes);
    target.sampled_large_read_bytes -= std::min(target.sampled_large_read_bytes, value.sampled_large_read_bytes);
    target.sampled_small_read_failures -=
        std::min(target.sampled_small_read_failures, value.sampled_small_read_failures);
    target.sampled_large_read_failures -=
        std::min(target.sampled_large_read_failures, value.sampled_large_read_failures);
    for (std::size_t index = 0; index < target.size_file_counts.size(); ++index) {
        target.size_file_counts[index] -= std::min(target.size_file_counts[index], value.size_file_counts[index]);
        target.size_logical_bytes[index] -= std::min(target.size_logical_bytes[index], value.size_logical_bytes[index]);
    }
    for (std::size_t index = 0; index < target.folder_fanout_counts.size(); ++index) {
        target.folder_fanout_counts[index] -=
            std::min(target.folder_fanout_counts[index], value.folder_fanout_counts[index]);
        target.files_per_folder_counts[index] -=
            std::min(target.files_per_folder_counts[index], value.files_per_folder_counts[index]);
        target.subdirs_per_folder_counts[index] -=
            std::min(target.subdirs_per_folder_counts[index], value.subdirs_per_folder_counts[index]);
    }
    for (std::size_t index = 0; index < target.folder_depth_counts.size(); ++index) {
        target.folder_depth_counts[index] -=
            std::min(target.folder_depth_counts[index], value.folder_depth_counts[index]);
    }
    for (std::size_t index = 0; index < target.entries_per_page_counts.size(); ++index) {
        target.entries_per_page_counts[index] -=
            std::min(target.entries_per_page_counts[index], value.entries_per_page_counts[index]);
    }
    for (std::size_t index = 0; index < target.readdirplus_page_latency_counts.size(); ++index) {
        target.readdirplus_page_latency_counts[index] -=
            std::min(target.readdirplus_page_latency_counts[index], value.readdirplus_page_latency_counts[index]);
        target.readdirplus_decode_latency_counts[index] -=
            std::min(target.readdirplus_decode_latency_counts[index], value.readdirplus_decode_latency_counts[index]);
        target.sampled_small_read_latency_counts[index] -=
            std::min(target.sampled_small_read_latency_counts[index], value.sampled_small_read_latency_counts[index]);
        target.sampled_large_read_latency_counts[index] -=
            std::min(target.sampled_large_read_latency_counts[index], value.sampled_large_read_latency_counts[index]);
    }
}

double small_ratio(const SyntheticProfileBuilder::Accumulator& accumulator) noexcept {
    if (accumulator.file_count == 0U) {
        return 0.0;
    }
    return static_cast<double>(accumulator.small_file_count) /
           static_cast<double>(accumulator.file_count);
}

SyntheticLatencyPercentiles approximate_latency_percentiles(
    const std::array<std::uint64_t, kSyntheticLatencyBucketCount>& buckets,
    std::uint64_t max_us) noexcept {
    SyntheticLatencyPercentiles result;
    result.max_us = max_us;
    std::uint64_t total = 0;
    for (const std::uint64_t count : buckets) {
        total += count;
    }
    if (total == 0U) {
        return result;
    }
    const auto bounds = synthetic_latency_bucket_bounds_us();
    const auto pick = [&](std::uint64_t numerator) {
        const std::uint64_t rank = std::max<std::uint64_t>(1U, (total * numerator + 99U) / 100U);
        std::uint64_t cursor = 0;
        for (std::size_t index = 0; index < buckets.size(); ++index) {
            cursor += buckets[index];
            if (cursor >= rank) {
                return bounds[index] == kSyntheticUnboundedSize ? max_us : bounds[index];
            }
        }
        return max_us;
    };
    result.p50_us = pick(50U);
    result.p90_us = pick(90U);
    result.p99_us = pick(99U);
    return result;
}

std::uint64_t scaled_count(std::uint64_t value, double scale) noexcept {
    if (scale <= 0.0 || !std::isfinite(scale)) {
        return value;
    }
    return std::max<std::uint64_t>(1U, static_cast<std::uint64_t>(
                                          std::llround(static_cast<double>(value) * scale)));
}

}  // namespace

std::array<std::uint64_t, kSyntheticSizeBucketCount> synthetic_size_bucket_bounds() noexcept {
    return {
        0,
        4ULL * 1024ULL,
        16ULL * 1024ULL,
        64ULL * 1024ULL,
        128ULL * 1024ULL,
        1024ULL * 1024ULL,
        16ULL * 1024ULL * 1024ULL,
        128ULL * 1024ULL * 1024ULL,
        1024ULL * 1024ULL * 1024ULL,
        kSyntheticUnboundedSize,
    };
}

std::array<std::uint64_t, kSyntheticLatencyBucketCount> synthetic_latency_bucket_bounds_us() noexcept {
    return {100, 500, 1'000, 5'000, 10'000, 50'000, 100'000, kSyntheticUnboundedSize};
}

std::size_t synthetic_size_bucket_index(std::uint64_t size_bytes) noexcept {
    const auto bounds = synthetic_size_bucket_bounds();
    for (std::size_t index = 0; index < bounds.size(); ++index) {
        if (size_bytes <= bounds[index]) {
            return index;
        }
    }
    return bounds.size() - 1U;
}

std::size_t synthetic_folder_fanout_bucket_index(std::uint64_t files_in_folder) noexcept {
    constexpr std::array<std::uint64_t, kSyntheticFolderFanoutBucketCount> bounds {
        0, 10, 100, 1'000, 10'000, 100'000, 1'000'000, kSyntheticUnboundedSize};
    for (std::size_t index = 0; index < bounds.size(); ++index) {
        if (files_in_folder <= bounds[index]) {
            return index;
        }
    }
    return bounds.size() - 1U;
}

std::size_t synthetic_folder_depth_bucket_index(std::uint64_t depth) noexcept {
    constexpr std::array<std::uint64_t, kSyntheticFolderDepthBucketCount> bounds {
        0, 1, 2, 3, 4, 6, 8, 12, 16, kSyntheticUnboundedSize};
    for (std::size_t index = 0; index < bounds.size(); ++index) {
        if (depth <= bounds[index]) {
            return index;
        }
    }
    return bounds.size() - 1U;
}

std::size_t synthetic_entries_per_page_bucket_index(std::uint64_t entries) noexcept {
    constexpr std::array<std::uint64_t, kSyntheticEntriesPerPageBucketCount> bounds {
        0, 16, 64, 256, 512, 1024, 4096, kSyntheticUnboundedSize};
    for (std::size_t index = 0; index < bounds.size(); ++index) {
        if (entries <= bounds[index]) {
            return index;
        }
    }
    return bounds.size() - 1U;
}

std::size_t synthetic_latency_bucket_index_us(std::uint64_t latency_us) noexcept {
    const auto bounds = synthetic_latency_bucket_bounds_us();
    for (std::size_t index = 0; index < bounds.size(); ++index) {
        if (latency_us <= bounds[index]) {
            return index;
        }
    }
    return bounds.size() - 1U;
}

std::uint64_t synthetic_splitmix64(std::uint64_t value) noexcept {
    value += 0x9e3779b97f4a7c15ULL;
    value = (value ^ (value >> 30U)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27U)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31U);
}

SyntheticProfileBuilder::SyntheticProfileBuilder(SyntheticProfileCaptureConfig config)
    : config_(config) {
    if (config_.block_file_count == 0U) {
        config_.block_file_count = 1;
    }
    config_.small_ratio_shift_threshold =
        std::clamp(config_.small_ratio_shift_threshold, 0.0, 1.0);
    profile_.seed = config_.seed;
    profile_.small_file_threshold_bytes = config_.small_file_threshold_bytes;
}

void SyntheticProfileBuilder::observe_file(const SyntheticObservedFile& file) {
    add_file_to_accumulator(active_, file, config_.small_file_threshold_bytes);
    add_file_to_accumulator(block_, file, config_.small_file_threshold_bytes);
    maybe_finish_block();
}

SyntheticWorkloadProfile SyntheticProfileBuilder::finish() {
    if (active_.file_count != 0U) {
        seal_active_phase();
    }
    return profile_;
}

const SyntheticProfileCaptureConfig& SyntheticProfileBuilder::config() const noexcept {
    return config_;
}

void SyntheticProfileBuilder::maybe_finish_block() {
    if (block_.file_count < config_.block_file_count) {
        return;
    }
    if (active_.file_count > block_.file_count) {
        Accumulator active_without_block = active_;
        subtract_accumulator(active_without_block, block_);
        const double delta = std::abs(small_ratio(block_) - small_ratio(active_without_block));
        if (delta > config_.small_ratio_shift_threshold) {
            active_ = active_without_block;
            seal_active_phase();
            active_ = block_;
        }
    }
    block_ = Accumulator {};
}

void SyntheticProfileBuilder::seal_active_phase() {
    if (active_.file_count == 0U) {
        return;
    }
    profile_.phases.push_back(make_phase(active_, profile_.phases.size()));
    active_ = Accumulator {};
}

SyntheticPhaseProfile SyntheticProfileBuilder::make_phase(const Accumulator& accumulator,
                                                          std::size_t index) const {
    SyntheticPhaseProfile phase;
    phase.name = "phase_" + std::to_string(index);
    phase.file_count = accumulator.file_count;
    phase.folder_count = accumulator.folder_count;
    phase.logical_size_bytes = accumulator.logical_size_bytes;
    phase.small_file_count = accumulator.small_file_count;
    phase.large_file_count = accumulator.large_file_count;
    phase.size_file_counts = accumulator.size_file_counts;
    phase.size_logical_bytes = accumulator.size_logical_bytes;
    phase.folder_fanout_counts = accumulator.folder_fanout_counts;
    phase.files_per_folder_counts = accumulator.files_per_folder_counts;
    phase.subdirs_per_folder_counts = accumulator.subdirs_per_folder_counts;
    phase.folder_depth_counts = accumulator.folder_depth_counts;
    phase.entries_per_page_counts = accumulator.entries_per_page_counts;
    phase.readdirplus_page_latency_counts = accumulator.readdirplus_page_latency_counts;
    phase.readdirplus_decode_latency_counts = accumulator.readdirplus_decode_latency_counts;
    phase.filename_length_sum = accumulator.filename_length_sum;
    phase.depth_sum = accumulator.depth_sum;
    phase.empty_folder_count = accumulator.empty_folder_count;
    phase.near_empty_folder_count = accumulator.near_empty_folder_count;
    phase.directory_count = accumulator.directory_count;
    phase.max_depth = accumulator.max_depth;
    phase.readdirplus_page_count = accumulator.readdirplus_page_count;
    phase.readdirplus_page_entries = accumulator.readdirplus_page_entries;
    phase.readdirplus_page_requested_bytes = accumulator.readdirplus_page_requested_bytes;
    phase.readdirplus_page_latency_sum_us = accumulator.readdirplus_page_latency_sum_us;
    phase.readdirplus_decode_latency_sum_us = accumulator.readdirplus_decode_latency_sum_us;
    phase.sampled_small_read_files = accumulator.sampled_small_read_files;
    phase.sampled_large_read_files = accumulator.sampled_large_read_files;
    phase.sampled_small_read_bytes = accumulator.sampled_small_read_bytes;
    phase.sampled_large_read_bytes = accumulator.sampled_large_read_bytes;
    phase.sampled_small_read_failures = accumulator.sampled_small_read_failures;
    phase.sampled_large_read_failures = accumulator.sampled_large_read_failures;
    phase.sampled_small_read_latency_counts = accumulator.sampled_small_read_latency_counts;
    phase.sampled_large_read_latency_counts = accumulator.sampled_large_read_latency_counts;
    phase.small_read_latency =
        approximate_latency_percentiles(accumulator.sampled_small_read_latency_counts, 0);
    phase.large_read_latency =
        approximate_latency_percentiles(accumulator.sampled_large_read_latency_counts, 0);
    return phase;
}

SyntheticReplayCursor::SyntheticReplayCursor(SyntheticReplayConfig config)
    : config_(std::move(config)) {}

bool SyntheticReplayCursor::next_file(SyntheticFileView& out) noexcept {
    const SyntheticPhaseProfile* phase = current_phase();
    while (phase != nullptr && phase_file_offset_ >= scaled_phase_files(*phase)) {
        ++phase_index_;
        phase_file_offset_ = 0;
        phase = current_phase();
    }
    if (phase == nullptr) {
        return false;
    }

    out = SyntheticFileView {};
    out.file_id = files_emitted_;
    out.folder_id = phase->folder_count == 0U ? 0U : files_emitted_ % phase->folder_count;
    out.size_bytes = sample_size(*phase, files_emitted_);
    out.small = out.size_bytes <= config_.profile.small_file_threshold_bytes;
    out.latency_us = sample_latency(*phase, out.small, files_emitted_);
    fill_path(out);
    fill_handle(out);

    ++files_emitted_;
    ++phase_file_offset_;
    bytes_emitted_ += out.size_bytes;
    return true;
}

std::uint64_t SyntheticReplayCursor::files_emitted() const noexcept {
    return files_emitted_;
}

std::uint64_t SyntheticReplayCursor::bytes_emitted() const noexcept {
    return bytes_emitted_;
}

const SyntheticPhaseProfile* SyntheticReplayCursor::current_phase() const noexcept {
    if (phase_index_ >= config_.profile.phases.size()) {
        return nullptr;
    }
    return &config_.profile.phases[phase_index_];
}

std::uint64_t SyntheticReplayCursor::scaled_phase_files(const SyntheticPhaseProfile& phase) const noexcept {
    return scaled_count(phase.file_count, config_.file_count_scale);
}

std::uint64_t SyntheticReplayCursor::sample_size(const SyntheticPhaseProfile& phase,
                                                 std::uint64_t file_id) const noexcept {
    std::uint64_t total = 0;
    for (const std::uint64_t count : phase.size_file_counts) {
        total += count;
    }
    if (total == 0U) {
        return 1;
    }
    const std::uint64_t choice = synthetic_splitmix64(config_.profile.seed ^ file_id) % total;
    std::uint64_t cursor = 0;
    std::size_t bucket = phase.size_file_counts.size() - 1U;
    for (std::size_t index = 0; index < phase.size_file_counts.size(); ++index) {
        cursor += phase.size_file_counts[index];
        if (choice < cursor) {
            bucket = index;
            break;
        }
    }
    const auto bounds = synthetic_size_bucket_bounds();
    const std::uint64_t upper = bounds[bucket];
    const std::uint64_t lower = bucket == 0U ? 0U : bounds[bucket - 1U] + 1U;
    const std::uint64_t width =
        upper == kSyntheticUnboundedSize ? std::max<std::uint64_t>(1U, lower) : upper - lower + 1U;
    std::uint64_t size = lower + (synthetic_splitmix64(config_.profile.seed + file_id * 17U) % width);
    if (upper == kSyntheticUnboundedSize) {
        size = lower + (synthetic_splitmix64(config_.profile.seed + file_id * 17U) % (1024ULL * 1024ULL * 1024ULL));
    }
    if (config_.data_size_scale > 0.0 && std::isfinite(config_.data_size_scale)) {
        size = std::max<std::uint64_t>(1U, static_cast<std::uint64_t>(
                                               std::llround(static_cast<double>(size) * config_.data_size_scale)));
    }
    return size;
}

std::uint64_t SyntheticReplayCursor::sample_latency(const SyntheticPhaseProfile& phase,
                                                    bool small,
                                                    std::uint64_t file_id) const noexcept {
    if (!config_.latency_enabled) {
        return 0;
    }
    const SyntheticLatencyPercentiles& latency =
        small ? phase.small_read_latency : phase.large_read_latency;
    const std::uint64_t pick = synthetic_splitmix64(config_.profile.seed ^ (file_id * 31U)) % 100U;
    if (pick < 50U) {
        return latency.p50_us;
    }
    if (pick < 90U) {
        return latency.p90_us;
    }
    if (pick < 99U) {
        return latency.p99_us;
    }
    return latency.max_us;
}

void SyntheticReplayCursor::fill_path(SyntheticFileView& out) const noexcept {
    char* cursor = out.path.data();
    cursor = append_literal(cursor, "synthetic/phase_");
    cursor = append_fixed_u64(cursor, phase_index_, 4U);
    cursor = append_literal(cursor, "/dir_");
    cursor = append_fixed_u64(cursor, out.folder_id, 10U);
    cursor = append_literal(cursor, "/file_");
    cursor = append_fixed_u64(cursor, out.file_id, 16U);
    cursor = append_literal(cursor, ".dat");
    out.path_length = static_cast<std::size_t>(cursor - out.path.data());
    if (out.path_length > out.path.size()) {
        out.path_length = out.path.size();
    }
}

void SyntheticReplayCursor::fill_handle(SyntheticFileView& out) const noexcept {
    for (std::size_t offset = 0; offset < out.nfs_handle.size(); offset += sizeof(std::uint64_t)) {
        const std::uint64_t word = synthetic_splitmix64(config_.profile.seed ^
                                                        (out.folder_id * 0x9e3779b97f4a7c15ULL) ^
                                                        (out.file_id + offset));
        std::memcpy(out.nfs_handle.data() + offset, &word, sizeof(word));
    }
}

SyntheticPayloadPool::SyntheticPayloadPool(std::size_t small_block_bytes,
                                           std::size_t large_block_bytes,
                                           SyntheticPayloadPattern pattern,
                                           std::uint64_t seed)
    : small_(std::max<std::size_t>(1U, small_block_bytes)),
      large_(std::max<std::size_t>(1U, large_block_bytes)) {
    fill(small_, pattern, seed ^ 0x51A11ULL);
    fill(large_, pattern, seed ^ 0x1A46EULL);
}

SyntheticPayloadView SyntheticPayloadPool::payload_for(const SyntheticFileView& file) const noexcept {
    if (file.small) {
        return small_payload(static_cast<std::size_t>(std::min<std::uint64_t>(
            file.size_bytes, static_cast<std::uint64_t>(small_.size()))));
    }
    return large_payload(static_cast<std::size_t>(std::min<std::uint64_t>(
        file.size_bytes, static_cast<std::uint64_t>(large_.size()))));
}

SyntheticPayloadView SyntheticPayloadPool::small_payload(std::size_t bytes) const noexcept {
    return SyntheticPayloadView {
        small_.data(),
        std::min(bytes, small_.size()),
        small_.size(),
    };
}

SyntheticPayloadView SyntheticPayloadPool::large_payload(std::size_t bytes) const noexcept {
    return SyntheticPayloadView {
        large_.data(),
        std::min(bytes, large_.size()),
        large_.size(),
    };
}

std::size_t SyntheticPayloadPool::small_block_bytes() const noexcept {
    return small_.size();
}

std::size_t SyntheticPayloadPool::large_block_bytes() const noexcept {
    return large_.size();
}

void SyntheticPayloadPool::fill(std::vector<std::uint8_t>& target,
                                SyntheticPayloadPattern pattern,
                                std::uint64_t seed) {
    switch (pattern) {
        case SyntheticPayloadPattern::zero:
            std::fill(target.begin(), target.end(), std::uint8_t {0});
            return;
        case SyntheticPayloadPattern::repeated:
            std::fill(target.begin(), target.end(), static_cast<std::uint8_t>(seed & 0xffU));
            return;
        case SyntheticPayloadPattern::deterministic:
            break;
    }
    for (std::size_t offset = 0; offset < target.size(); ++offset) {
        target[offset] = static_cast<std::uint8_t>(
            synthetic_splitmix64(seed + static_cast<std::uint64_t>(offset)) & 0xffU);
    }
}

}  // namespace hypersync
