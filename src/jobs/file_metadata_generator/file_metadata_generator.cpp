#include "jobs/file_metadata_generator/file_metadata_generator.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <string_view>
#include <stdexcept>
#include <utility>

namespace hypersync {
namespace {

std::uint64_t splitmix64(std::uint64_t value) noexcept {
    value += 0x9e3779b97f4a7c15ULL;
    value = (value ^ (value >> 30U)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27U)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31U);
}

char* append_literal(char* out, std::string_view value) {
    std::memcpy(out, value.data(), value.size());
    return out + value.size();
}

char* append_fixed_u64(char* out, std::uint64_t value, std::size_t width) {
    for (std::size_t offset = 0; offset < width; ++offset) {
        const std::size_t digit_index = width - 1U - offset;
        out[digit_index] = static_cast<char>('0' + (value % 10U));
        value /= 10U;
    }
    return out + width;
}

std::string_view format_file_path_view(const std::string& root,
                                       std::uint64_t folder_index,
                                       std::uint64_t file_index,
                                       std::array<char, 256>& buffer) {
    constexpr std::size_t kSuffixBytes = 5U + 8U + 6U + 12U + 4U;
    if (root.size() + kSuffixBytes > buffer.size()) {
        throw std::runtime_error("generated metadata path exceeded fixed formatter buffer");
    }
    char* out = buffer.data();
    out = append_literal(out, root);
    out = append_literal(out, "/dir_");
    out = append_fixed_u64(out, folder_index, 8U);
    out = append_literal(out, "/file_");
    out = append_fixed_u64(out, file_index, 12U);
    out = append_literal(out, ".dat");
    if (static_cast<std::size_t>(out - buffer.data()) > buffer.size()) {
        throw std::runtime_error("generated metadata path exceeded fixed formatter buffer");
    }
    return std::string_view(buffer.data(), static_cast<std::size_t>(out - buffer.data()));
}

std::string_view format_folder_path_view(const std::string& root,
                                         std::uint64_t folder_index,
                                         std::array<char, 256>& buffer) {
    constexpr std::size_t kSuffixBytes = 5U + 8U;
    if (root.size() + kSuffixBytes > buffer.size()) {
        throw std::runtime_error("generated metadata path exceeded fixed formatter buffer");
    }
    char* out = buffer.data();
    out = append_literal(out, root);
    out = append_literal(out, "/dir_");
    out = append_fixed_u64(out, folder_index, 8U);
    if (static_cast<std::size_t>(out - buffer.data()) > buffer.size()) {
        throw std::runtime_error("generated metadata path exceeded fixed formatter buffer");
    }
    return std::string_view(buffer.data(), static_cast<std::size_t>(out - buffer.data()));
}

std::string format_path(const char* format,
                        const std::string& root,
                        std::uint64_t first,
                        std::uint64_t second) {
    std::array<char, 256> buffer {};
    std::string_view path;
    if (std::string_view(format) == "%s/dir_%08llu/file_%012llu.dat") {
        path = format_file_path_view(root, first, second, buffer);
    } else if (std::string_view(format) == "%s/dir_%08llu") {
        path = format_folder_path_view(root, first, buffer);
    } else {
        throw std::runtime_error("unknown generated metadata path format");
    }
    return std::string(path);
}

}  // namespace

FileMetadataGeneratorConfig::FileMetadataGeneratorConfig() = default;

FileMetadataGeneratorConfig::FileMetadataGeneratorConfig(std::uint64_t file_count,
                                                         std::uint64_t folder_count,
                                                         std::size_t batch_size,
                                                         std::uint64_t average_file_size,
                                                         std::uint64_t seed,
                                                         std::string root_prefix)
    : file_count(file_count),
      folder_count(folder_count),
      batch_size(batch_size),
      average_file_size(average_file_size),
      seed(seed),
      root_prefix(std::move(root_prefix)) {
    if (this->batch_size == 0U) {
        throw std::invalid_argument("file metadata generator batch size must be positive");
    }
    if (this->root_prefix.empty()) {
        throw std::invalid_argument("file metadata generator root prefix must not be empty");
    }
}

FileMetadataGenerator::FileMetadataGenerator(FileMetadataGeneratorConfig config)
    : config_(std::move(config)) {
    if (config_.batch_size == 0U) {
        throw std::invalid_argument("file metadata generator batch size must be positive");
    }
    if (config_.root_prefix.empty()) {
        throw std::invalid_argument("file metadata generator root prefix must not be empty");
    }
}

bool FileMetadataGenerator::next_batch(FileMetadataGeneratorBatch& out) {
    out.files.clear();
    out.folders.clear();

    if (next_file_ >= config_.file_count && next_folder_ >= config_.folder_count) {
        return false;
    }

    const std::uint64_t files_left = config_.file_count - std::min(next_file_, config_.file_count);
    const std::size_t files_to_emit =
        static_cast<std::size_t>(std::min<std::uint64_t>(files_left, config_.batch_size));
    out.files.reserve(files_to_emit);
    for (std::size_t index = 0; index < files_to_emit; ++index) {
        out.files.push_back(make_file(next_file_++));
    }

    // Folder records are much fewer than file records in typical inventories.
    // Emit them alongside early batches so writer benchmarks include both row
    // shapes without allowing folder generation to dominate the hot path.
    const std::uint64_t folders_left = config_.folder_count - std::min(next_folder_, config_.folder_count);
    const std::size_t folder_budget = std::max<std::size_t>(1U, config_.batch_size / 1024U);
    const std::size_t folders_to_emit =
        static_cast<std::size_t>(std::min<std::uint64_t>(folders_left, folder_budget));
    out.folders.reserve(folders_to_emit);
    for (std::size_t index = 0; index < folders_to_emit; ++index) {
        out.folders.push_back(make_folder(next_folder_++));
    }

    return !out.files.empty() || !out.folders.empty();
}

std::uint64_t FileMetadataGenerator::files_generated() const noexcept {
    return next_file_;
}

std::uint64_t FileMetadataGenerator::folders_generated() const noexcept {
    return next_folder_;
}

const FileMetadataGeneratorConfig& FileMetadataGenerator::config() const noexcept {
    return config_;
}

void FileMetadataGenerator::for_each_file_view(
    std::uint64_t first,
    std::uint64_t count,
    const std::function<void(const GeneratedFileMetadataView&)>& callback) const {
    const std::uint64_t folder_count = std::max<std::uint64_t>(1U, config_.folder_count);
    std::array<char, 256> path {};
    for (std::uint64_t index = first; index < first + count; ++index) {
        const std::uint64_t folder_index = index % folder_count;
        GeneratedFileMetadataView view;
        view.rel_path = format_file_path_view(config_.root_prefix, folder_index, index, path);
        view.declared_size = file_size_for(index);
        view.mtime = 1'700'000'000'000'000'000ULL + index;
        view.mode = 0644;
        view.uid = static_cast<std::uint32_t>(1000U + (index % 97U));
        view.gid = static_cast<std::uint32_t>(1000U + (index % 89U));
        callback(view);
    }
}

void FileMetadataGenerator::for_each_folder_view(
    std::uint64_t first,
    std::uint64_t count,
    const std::function<void(const GeneratedFolderMetadataView&)>& callback) const {
    std::array<char, 256> path {};
    for (std::uint64_t index = first; index < first + count; ++index) {
        callback(make_folder_view(index, format_folder_path_view(config_.root_prefix, index, path)));
    }
}

FileSpec FileMetadataGenerator::make_file(std::uint64_t index) const {
    const std::uint64_t folder_count = std::max<std::uint64_t>(1U, config_.folder_count);
    const std::uint64_t folder_index = index % folder_count;
    FileSpec spec;
    spec.rel_path = format_path("%s/dir_%08llu/file_%012llu.dat", config_.root_prefix, folder_index, index);
    spec.declared_size = file_size_for(index);
    spec.mtime = 1'700'000'000'000'000'000ULL + index;
    spec.mode = 0644;
    spec.uid = static_cast<std::uint32_t>(1000U + (index % 97U));
    spec.gid = static_cast<std::uint32_t>(1000U + (index % 89U));
    return spec;
}

MetadataFolderRecord FileMetadataGenerator::make_folder(std::uint64_t index) const {
    MetadataFolderRecord folder;
    const std::string path = folder_path(index);
    const GeneratedFolderMetadataView view = make_folder_view(index, path);
    folder.spec.rel_path = path;
    folder.spec.mtime = view.mtime;
    folder.spec.mode = view.mode;
    folder.spec.uid = view.uid;
    folder.spec.gid = view.gid;
    folder.flat_file_count = static_cast<std::size_t>(view.flat_file_count);
    folder.flat_logical_size_bytes = view.flat_logical_size_bytes;
    return folder;
}

std::uint64_t FileMetadataGenerator::file_size_for(std::uint64_t index) const noexcept {
    if (config_.average_file_size == 0U) {
        return 0;
    }
    const std::uint64_t jitter = splitmix64(config_.seed ^ index) % (config_.average_file_size * 2U);
    return std::max<std::uint64_t>(1U, jitter);
}

GeneratedFolderMetadataView FileMetadataGenerator::make_folder_view(std::uint64_t index, std::string_view path) const {
    GeneratedFolderMetadataView folder;
    folder.rel_path = path;
    folder.mtime = 1'700'000'000'000'000'000ULL + index;
    folder.mode = 0755;
    folder.uid = static_cast<std::uint32_t>(1000U + (index % 97U));
    folder.gid = static_cast<std::uint32_t>(1000U + (index % 89U));
    const std::uint64_t base_count = config_.folder_count == 0U ? 0U : config_.file_count / config_.folder_count;
    const std::uint64_t remainder = config_.folder_count == 0U ? 0U : config_.file_count % config_.folder_count;
    folder.flat_file_count = base_count + (index < remainder ? 1U : 0U);
    folder.flat_logical_size_bytes = folder.flat_file_count * config_.average_file_size;
    return folder;
}

std::string FileMetadataGenerator::folder_path(std::uint64_t folder_index) const {
    return format_path("%s/dir_%08llu", config_.root_prefix, folder_index, 0);
}

}  // namespace hypersync
