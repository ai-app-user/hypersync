#ifndef HYPERSYNC_JOBS_FILE_METADATA_GENERATOR_HPP
#define HYPERSYNC_JOBS_FILE_METADATA_GENERATOR_HPP

// Synthetic metadata generator for writer and pipeline performance tests.
//
// This job-adjacent helper creates deterministic FileSpec and folder records in
// bounded batches. It does not touch NFS and does not keep the whole generated
// tree in memory, so it can feed output writers at high speed.

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "core/metadata_record_writer.hpp"

namespace hypersync {

struct FileMetadataGeneratorConfig {
    std::uint64_t file_count = 1'000'000;
    std::uint64_t folder_count = 1'000;
    std::size_t batch_size = 65'536;
    std::uint64_t average_file_size = 32 * 1024;
    std::uint64_t seed = 0x9e3779b97f4a7c15ULL;
    std::string root_prefix = "generated";

    FileMetadataGeneratorConfig();
    FileMetadataGeneratorConfig(std::uint64_t file_count,
                                std::uint64_t folder_count,
                                std::size_t batch_size,
                                std::uint64_t average_file_size,
                                std::uint64_t seed,
                                std::string root_prefix);
};

struct FileMetadataGeneratorBatch {
    std::vector<FileSpec> files;
    std::vector<MetadataFolderRecord> folders;
};

struct GeneratedFileMetadataView {
    std::string_view rel_path;
    std::uint64_t declared_size = 0;
    std::uint64_t mtime = 0;
    std::uint32_t mode = 0644;
    std::uint32_t uid = 0;
    std::uint32_t gid = 0;
};

struct GeneratedFolderMetadataView {
    std::string_view rel_path;
    std::uint64_t mtime = 0;
    std::uint32_t mode = 0755;
    std::uint32_t uid = 0;
    std::uint32_t gid = 0;
    std::uint64_t flat_file_count = 0;
    std::uint64_t flat_logical_size_bytes = 0;
};

class FileMetadataGenerator {
public:
    explicit FileMetadataGenerator(FileMetadataGeneratorConfig config);

    // Fill the output batch with the next bounded set of synthetic records.
    // Returns false after all configured files and folders have been emitted.
    bool next_batch(FileMetadataGeneratorBatch& out);

    [[nodiscard]] std::uint64_t files_generated() const noexcept;
    [[nodiscard]] std::uint64_t folders_generated() const noexcept;
    [[nodiscard]] const FileMetadataGeneratorConfig& config() const noexcept;
    void for_each_file_view(std::uint64_t first,
                            std::uint64_t count,
                            const std::function<void(const GeneratedFileMetadataView&)>& callback) const;
    void for_each_folder_view(std::uint64_t first,
                              std::uint64_t count,
                              const std::function<void(const GeneratedFolderMetadataView&)>& callback) const;

private:
    [[nodiscard]] FileSpec make_file(std::uint64_t index) const;
    [[nodiscard]] MetadataFolderRecord make_folder(std::uint64_t index) const;
    [[nodiscard]] std::uint64_t file_size_for(std::uint64_t index) const noexcept;
    [[nodiscard]] GeneratedFolderMetadataView make_folder_view(std::uint64_t index, std::string_view path) const;
    [[nodiscard]] std::string folder_path(std::uint64_t folder_index) const;

    FileMetadataGeneratorConfig config_;
    std::uint64_t next_file_ = 0;
    std::uint64_t next_folder_ = 0;
};

}  // namespace hypersync

#endif
