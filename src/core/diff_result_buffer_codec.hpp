#ifndef HYPERSYNC_CORE_DIFF_RESULT_BUFFER_CODEC_HPP
#define HYPERSYNC_CORE_DIFF_RESULT_BUFFER_CODEC_HPP

// Folder-level diff result payload carried in MetadataBatchBuffer.
//
// A result buffer may contain many folder summaries. The payload is a domain
// view over an owned generic buffer; transport and queue layers remain generic.

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string_view>

#include "core/pipeline_buffers.hpp"

namespace hypersync {

struct FolderDiffSummary {
    std::string_view rel_path;
    std::string_view error;
    std::uint64_t source_scan_started_unix_ns = 0;
    std::uint64_t source_scan_finished_unix_ns = 0;
    std::uint64_t target_scan_started_unix_ns = 0;
    std::uint64_t target_scan_finished_unix_ns = 0;
    std::uint64_t result_sent_unix_ns = 0;
    std::uint64_t result_received_unix_ns = 0;
    std::uint64_t source_file_count = 0;
    std::uint64_t target_file_count = 0;
    std::uint64_t source_folder_count = 0;
    std::uint64_t target_folder_count = 0;
    std::uint64_t files_same = 0;
    std::uint64_t files_changed = 0;
    std::uint64_t files_new = 0;
    std::uint64_t files_target_only = 0;
    std::uint64_t files_failed = 0;
    std::uint64_t source_logical_size_bytes = 0;
    std::uint64_t target_logical_size_bytes = 0;
    std::uint64_t same_logical_size_bytes = 0;
    std::uint64_t changed_logical_size_bytes = 0;
    std::uint64_t new_logical_size_bytes = 0;
    std::uint64_t target_only_logical_size_bytes = 0;
    std::uint64_t bytes_planned = 0;
    std::uint8_t status = 0;
};

void reset_diff_result_buffer(MetadataBatchBuffer& buffer);
[[nodiscard]] bool is_diff_result_buffer(const MetadataBatchBuffer& buffer);
[[nodiscard]] std::size_t diff_result_transport_size(const MetadataBatchBuffer& buffer);
[[nodiscard]] bool append_diff_result(MetadataBatchBuffer& buffer, const FolderDiffSummary& summary);
void visit_diff_results(const MetadataBatchBuffer& buffer,
                        const std::function<void(FolderDiffSummary)>& visitor);

}  // namespace hypersync

#endif
