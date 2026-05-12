#ifndef HYPERSYNC_COMMON_FILESYSTEM_UTILS_HPP
#define HYPERSYNC_COMMON_FILESYSTEM_UTILS_HPP

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "common/types.hpp"

namespace hypersync {

std::vector<FileSpec> collect_file_specs(const std::filesystem::path& source_root, bool recursive = true);
std::vector<FileSpec> collect_directory_specs(const std::filesystem::path& source_root, bool recursive = true);
std::string read_file_contents(const std::filesystem::path& path);
std::uint64_t file_hash64(const std::filesystem::path& path);
void ensure_parent_directories(const std::filesystem::path& path);
void apply_file_metadata(const std::filesystem::path& path, const FileSpec& spec);
void apply_directory_metadata(const std::filesystem::path& path, const FileSpec& spec);

}  // namespace hypersync

#endif
