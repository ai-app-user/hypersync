#ifndef HYPERSYNC_CORE_FLAT_FOLDER_BUFFER_CODEC_HPP
#define HYPERSYNC_CORE_FLAT_FOLDER_BUFFER_CODEC_HPP

// Compact flat-folder metadata payload carried inside MetadataBatchBuffer.
//
// This codec is intentionally a payload view over a generic buffer. Queues and
// transport jobs still see only BufferHandle values. The folder path is stored
// once per buffer; child records store compact basename metadata and can be
// split across sequence-numbered buffers for very large flat folders.

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string_view>
#include <vector>

#include "common/types.hpp"
#include "core/pipeline_buffers.hpp"

namespace hypersync {

struct FlatFolderBufferInfo {
    std::string_view folder_path;
    std::string_view error;
    std::uint32_t sequence = 0;
    std::uint32_t child_record_count = 0;
    std::uint64_t folder_hash = 0;
    std::uint64_t folder_mtime = 0;
    std::uint32_t folder_mode = 0;
    std::uint32_t folder_uid = 0;
    std::uint32_t folder_gid = 0;
    std::uint64_t scan_started_unix_ns = 0;
    std::uint64_t scan_finished_unix_ns = 0;
    std::uint64_t total_file_count = 0;
    std::uint64_t total_folder_count = 0;
    std::uint64_t total_logical_size_bytes = 0;
    std::uint64_t metadata_hash = 0;
    bool final_batch = false;
    bool failed = false;
};

struct FlatFolderChildView {
    std::string_view name;
    std::uint64_t name_hash = 0;
    std::uint64_t metadata_hash = 0;
    std::uint64_t logical_size = 0;
    std::uint64_t mtime = 0;
    std::uint32_t mode = 0;
    std::uint32_t uid = 0;
    std::uint32_t gid = 0;
    std::uint64_t flat_file_count = 0;
    std::uint64_t flat_logical_size_bytes = 0;
    bool is_file = false;
};

[[nodiscard]] bool is_flat_folder_buffer(const MetadataBatchBuffer& buffer);
[[nodiscard]] FlatFolderBufferInfo flat_folder_buffer_info(const MetadataBatchBuffer& buffer);
[[nodiscard]] std::size_t flat_folder_transport_size(const MetadataBatchBuffer& buffer);

void reset_flat_folder_buffer(MetadataBatchBuffer& buffer,
                              const FileSpec& folder,
                              std::uint32_t sequence,
                              bool final_batch,
                              bool failed,
                              std::string_view error,
                              std::uint64_t total_file_count,
                              std::uint64_t total_folder_count,
                              std::uint64_t total_logical_size_bytes,
                              std::uint64_t metadata_hash,
                              std::uint64_t scan_started_unix_ns,
                              std::uint64_t scan_finished_unix_ns);

void set_flat_folder_buffer_final(MetadataBatchBuffer& buffer, bool final_batch);

[[nodiscard]] bool append_flat_folder_file(MetadataBatchBuffer& buffer,
                                           const FileSpec& file,
                                           std::string_view compare_mode);
[[nodiscard]] bool append_flat_folder_folder(MetadataBatchBuffer& buffer,
                                             const FileSpec& folder,
                                             std::string_view compare_mode);

void visit_flat_folder_children(const MetadataBatchBuffer& buffer,
                                const std::function<void(FlatFolderChildView)>& visitor);

[[nodiscard]] std::uint64_t flat_folder_metadata_hash(const std::vector<FileSpec>& files,
                                                      const std::vector<FileSpec>& folders,
                                                      std::string_view compare_mode);
[[nodiscard]] std::uint64_t flat_folder_logical_size(const std::vector<FileSpec>& files);

}  // namespace hypersync

#endif
