#include "common/hash_utils.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <utility>
#include <vector>

#include "common/path_utils.hpp"

namespace hypersync {

namespace {

constexpr std::uint64_t kXxhPrime1 = 11400714785074694791ULL;
constexpr std::uint64_t kXxhPrime2 = 14029467366897019727ULL;
constexpr std::uint64_t kXxhPrime3 = 1609587929392839161ULL;
constexpr std::uint64_t kXxhPrime4 = 9650029242287828579ULL;
constexpr std::uint64_t kXxhPrime5 = 2870177450012600261ULL;

std::uint64_t rotate_left(std::uint64_t value, unsigned shift) {
    return (value << shift) | (value >> (64U - shift));
}

std::uint64_t read_u64_le(const char* data) {
    std::uint64_t value = 0;
    std::memcpy(&value, data, sizeof(value));
    return value;
}

std::uint32_t read_u32_le(const char* data) {
    std::uint32_t value = 0;
    std::memcpy(&value, data, sizeof(value));
    return value;
}

std::uint64_t xxh64_round(std::uint64_t acc, std::uint64_t lane) {
    acc += lane * kXxhPrime2;
    acc = rotate_left(acc, 31U);
    acc *= kXxhPrime1;
    return acc;
}

std::uint64_t xxh64_merge_round(std::uint64_t acc, std::uint64_t value) {
    acc ^= xxh64_round(0, value);
    acc = acc * kXxhPrime1 + kXxhPrime4;
    return acc;
}

std::uint64_t xxh64_avalanche(std::uint64_t value) {
    value ^= value >> 33U;
    value *= kXxhPrime2;
    value ^= value >> 29U;
    value *= kXxhPrime3;
    value ^= value >> 32U;
    return value;
}

std::uint64_t hash_u64(std::uint64_t seed, std::uint64_t value) {
    seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
    return seed;
}

}  // namespace

Hash64State::Hash64State()
    : lane1_(kXxhPrime1 + kXxhPrime2),
      lane2_(kXxhPrime2),
      lane3_(0),
      lane4_(0 - kXxhPrime1) {}

void Hash64State::process_lanes(const char* bytes) {
    lane1_ = xxh64_round(lane1_, read_u64_le(bytes));
    lane2_ = xxh64_round(lane2_, read_u64_le(bytes + 8));
    lane3_ = xxh64_round(lane3_, read_u64_le(bytes + 16));
    lane4_ = xxh64_round(lane4_, read_u64_le(bytes + 24));
}

void Hash64State::update(std::string_view input) {
    const char* bytes = input.data();
    std::size_t remaining = input.size();
    total_bytes_ += remaining;

    if (buffered_bytes_ + remaining < buffer_.size()) {
        std::memcpy(buffer_.data() + buffered_bytes_, bytes, remaining);
        buffered_bytes_ += remaining;
        return;
    }

    if (buffered_bytes_ != 0U) {
        const std::size_t fill = buffer_.size() - buffered_bytes_;
        std::memcpy(buffer_.data() + buffered_bytes_, bytes, fill);
        process_lanes(buffer_.data());
        bytes += fill;
        remaining -= fill;
        buffered_bytes_ = 0;
    }

    while (remaining >= buffer_.size()) {
        process_lanes(bytes);
        bytes += buffer_.size();
        remaining -= buffer_.size();
    }

    if (remaining != 0U) {
        std::memcpy(buffer_.data(), bytes, remaining);
        buffered_bytes_ = remaining;
    }
}

std::uint64_t Hash64State::value() const {
    std::uint64_t hash = 0;
    if (total_bytes_ >= buffer_.size()) {
        hash = rotate_left(lane1_, 1U) +
               rotate_left(lane2_, 7U) +
               rotate_left(lane3_, 12U) +
               rotate_left(lane4_, 18U);
        hash = xxh64_merge_round(hash, lane1_);
        hash = xxh64_merge_round(hash, lane2_);
        hash = xxh64_merge_round(hash, lane3_);
        hash = xxh64_merge_round(hash, lane4_);
    } else {
        hash = kXxhPrime5;
    }

    hash += total_bytes_;

    const char* bytes = buffer_.data();
    std::size_t remaining = buffered_bytes_;
    while (remaining >= 8U) {
        const std::uint64_t lane = read_u64_le(bytes);
        hash ^= xxh64_round(0, lane);
        hash = rotate_left(hash, 27U) * kXxhPrime1 + kXxhPrime4;
        bytes += 8;
        remaining -= 8;
    }
    if (remaining >= 4U) {
        hash ^= static_cast<std::uint64_t>(read_u32_le(bytes)) * kXxhPrime1;
        hash = rotate_left(hash, 23U) * kXxhPrime2 + kXxhPrime3;
        bytes += 4;
        remaining -= 4;
    }
    while (remaining != 0U) {
        hash ^= static_cast<std::uint64_t>(static_cast<unsigned char>(*bytes)) * kXxhPrime5;
        hash = rotate_left(hash, 11U) * kXxhPrime1;
        ++bytes;
        --remaining;
    }

    return xxh64_avalanche(hash);
}

std::uint64_t hash64(std::string_view input) {
    Hash64State hasher;
    hasher.update(input);
    return hasher.value();
}

std::uint32_t chunk_hash32(std::string_view input) {
    const std::uint64_t full_hash = hash64(input);
    return static_cast<std::uint32_t>(full_hash ^ (full_hash >> 32U));
}

std::uint64_t path_hash(std::string_view name, std::uint64_t parent_hash) {
    return hash_u64(hash64(name), parent_hash);
}

std::uint64_t folder_hash_for_path(std::string_view path) {
    const std::string normalized = normalize_path(path);
    if (normalized.empty()) {
        return 0;
    }

    std::uint64_t current = 0;
    std::string component;
    for (char ch : normalized) {
        if (ch == '/') {
            current = path_hash(component, current);
            component.clear();
        } else {
            component.push_back(ch);
        }
    }
    if (!component.empty()) {
        current = path_hash(component, current);
    }
    return current;
}

std::uint64_t compute_folder_data_hash(const std::vector<FileSnapshot>& entries) {
    if (entries.empty()) {
        return 0;
    }
    std::vector<std::pair<std::uint64_t, std::uint64_t>> pairs;
    pairs.reserve(entries.size());
    for (const auto& entry : entries) {
        pairs.emplace_back(entry.file_hash, entry.data_hash);
    }
    std::sort(pairs.begin(), pairs.end());
    std::uint64_t combined = kXxhPrime5;
    for (const auto& [file_hash, data_hash] : pairs) {
        combined = hash_u64(combined, file_hash);
        combined = hash_u64(combined, data_hash);
    }
    return combined;
}

}  // namespace hypersync
