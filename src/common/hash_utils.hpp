#ifndef HYPERSYNC_COMMON_HASH_UTILS_HPP
#define HYPERSYNC_COMMON_HASH_UTILS_HPP

#include <array>
#include <cstdint>
#include <string_view>
#include <vector>

#include "common/types.hpp"

namespace hypersync {

class Hash64State {
public:
    Hash64State();

    void update(std::string_view input);
    [[nodiscard]] std::uint64_t value() const;

private:
    void process_lanes(const char* bytes);

    std::array<char, 32> buffer_ {};
    std::size_t buffered_bytes_ = 0;
    std::uint64_t total_bytes_ = 0;
    std::uint64_t lane1_ = 0;
    std::uint64_t lane2_ = 0;
    std::uint64_t lane3_ = 0;
    std::uint64_t lane4_ = 0;
};

std::uint64_t hash64(std::string_view input);
std::uint32_t chunk_hash32(std::string_view input);
std::uint64_t path_hash(std::string_view name, std::uint64_t parent_hash);
std::uint64_t folder_hash_for_path(std::string_view path);
std::uint64_t compute_folder_data_hash(const std::vector<FileSnapshot>& entries);

}  // namespace hypersync

#endif
