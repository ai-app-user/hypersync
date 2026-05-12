#ifndef HYPERSYNC_COMMON_SCAN_INDEX_HPP
#define HYPERSYNC_COMMON_SCAN_INDEX_HPP

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/types.hpp"

namespace hypersync {

class ScanIndex {
public:
    void add(FileSnapshot snapshot);
    [[nodiscard]] bool empty() const;
    [[nodiscard]] std::optional<FileSnapshot> find(std::string_view rel_path) const;
    [[nodiscard]] bool file_matches(const FileSnapshot& snapshot) const;
    [[nodiscard]] bool metadata_matches(std::string_view rel_path, std::uint64_t size, std::uint64_t mtime) const;
    [[nodiscard]] std::uint64_t folder_data_hash(std::string_view folder_path) const;
    [[nodiscard]] std::vector<FileSnapshot> rows() const;
    [[nodiscard]] std::string to_csv() const;

    static ScanIndex from_csv(std::string_view csv);

private:
    std::unordered_map<std::string, FileSnapshot> rows_by_path_;
    std::unordered_map<std::string, std::vector<std::string>> folder_index_;
};

}  // namespace hypersync

#endif
