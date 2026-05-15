#include "core/diff_result_buffer_codec.hpp"

#include <cstring>
#include <limits>
#include <stdexcept>

namespace hypersync {
namespace {

struct DiffResultBufferHeader {
    std::uint32_t magic = 0;
    std::uint16_t version = 0;
    std::uint16_t reserved = 0;
    std::uint32_t record_count = 0;
    std::uint32_t reserved2 = 0;
};

struct DiffResultRecordHeader {
    std::uint32_t path_bytes = 0;
    std::uint32_t error_bytes = 0;
    std::uint64_t source_scan_started_unix_ns = 0;
    std::uint64_t source_scan_finished_unix_ns = 0;
    std::uint64_t target_scan_started_unix_ns = 0;
    std::uint64_t target_scan_finished_unix_ns = 0;
    std::uint64_t result_sent_unix_ns = 0;
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
    std::uint8_t reserved[7] {};
};

inline constexpr std::uint32_t kDiffResultMagic = 0x31524644U;  // "DFR1" little-endian.
inline constexpr std::uint16_t kDiffResultVersion = 1U;

DiffResultBufferHeader read_header(const MetadataBatchBuffer& buffer) {
    if (buffer.bytes_used < sizeof(DiffResultBufferHeader)) {
        throw std::runtime_error("diff result buffer header is truncated");
    }
    DiffResultBufferHeader header {};
    std::memcpy(&header, buffer.bytes.data(), sizeof(header));
    if (header.magic != kDiffResultMagic || header.version != kDiffResultVersion) {
        throw std::runtime_error("diff result buffer header is invalid");
    }
    return header;
}

void write_header(MetadataBatchBuffer& buffer, const DiffResultBufferHeader& header) {
    std::memcpy(buffer.bytes.data(), &header, sizeof(header));
}

}  // namespace

void reset_diff_result_buffer(MetadataBatchBuffer& buffer) {
    buffer.bytes_used = sizeof(DiffResultBufferHeader);
    buffer.record_count = 0;
    DiffResultBufferHeader header {};
    header.magic = kDiffResultMagic;
    header.version = kDiffResultVersion;
    write_header(buffer, header);
}

bool is_diff_result_buffer(const MetadataBatchBuffer& buffer) {
    if (buffer.bytes_used < sizeof(std::uint32_t)) {
        return false;
    }
    std::uint32_t magic = 0;
    std::memcpy(&magic, buffer.bytes.data(), sizeof(magic));
    return magic == kDiffResultMagic;
}

std::size_t diff_result_transport_size(const MetadataBatchBuffer& buffer) {
    return sizeof(std::uint32_t) + sizeof(std::uint32_t) + buffer.bytes_used;
}

bool append_diff_result(MetadataBatchBuffer& buffer, const FolderDiffSummary& summary) {
    if (summary.rel_path.size() > std::numeric_limits<std::uint32_t>::max() ||
        summary.error.size() > std::numeric_limits<std::uint32_t>::max()) {
        return false;
    }
    const std::size_t needed = sizeof(DiffResultRecordHeader) + summary.rel_path.size() + summary.error.size();
    if (needed > kMetadataBatchBufferBytes - buffer.bytes_used) {
        return false;
    }

    DiffResultRecordHeader record {};
    record.path_bytes = static_cast<std::uint32_t>(summary.rel_path.size());
    record.error_bytes = static_cast<std::uint32_t>(summary.error.size());
    record.source_scan_started_unix_ns = summary.source_scan_started_unix_ns;
    record.source_scan_finished_unix_ns = summary.source_scan_finished_unix_ns;
    record.target_scan_started_unix_ns = summary.target_scan_started_unix_ns;
    record.target_scan_finished_unix_ns = summary.target_scan_finished_unix_ns;
    record.result_sent_unix_ns = summary.result_sent_unix_ns;
    record.source_file_count = summary.source_file_count;
    record.target_file_count = summary.target_file_count;
    record.source_folder_count = summary.source_folder_count;
    record.target_folder_count = summary.target_folder_count;
    record.files_same = summary.files_same;
    record.files_changed = summary.files_changed;
    record.files_new = summary.files_new;
    record.files_target_only = summary.files_target_only;
    record.files_failed = summary.files_failed;
    record.source_logical_size_bytes = summary.source_logical_size_bytes;
    record.target_logical_size_bytes = summary.target_logical_size_bytes;
    record.same_logical_size_bytes = summary.same_logical_size_bytes;
    record.changed_logical_size_bytes = summary.changed_logical_size_bytes;
    record.new_logical_size_bytes = summary.new_logical_size_bytes;
    record.target_only_logical_size_bytes = summary.target_only_logical_size_bytes;
    record.bytes_planned = summary.bytes_planned;
    record.status = summary.status;

    auto* cursor = buffer.bytes.data() + buffer.bytes_used;
    std::memcpy(cursor, &record, sizeof(record));
    cursor += sizeof(record);
    std::memcpy(cursor, summary.rel_path.data(), summary.rel_path.size());
    cursor += summary.rel_path.size();
    std::memcpy(cursor, summary.error.data(), summary.error.size());
    buffer.bytes_used += static_cast<std::uint32_t>(needed);
    ++buffer.record_count;

    DiffResultBufferHeader header = read_header(buffer);
    ++header.record_count;
    write_header(buffer, header);
    return true;
}

void visit_diff_results(const MetadataBatchBuffer& buffer,
                        const std::function<void(FolderDiffSummary)>& visitor) {
    const DiffResultBufferHeader header = read_header(buffer);
    std::size_t offset = sizeof(DiffResultBufferHeader);
    std::uint32_t seen = 0;
    while (offset < buffer.bytes_used) {
        if (buffer.bytes_used - offset < sizeof(DiffResultRecordHeader)) {
            throw std::runtime_error("diff result record header is truncated");
        }
        DiffResultRecordHeader record {};
        std::memcpy(&record, buffer.bytes.data() + offset, sizeof(record));
        offset += sizeof(record);
        const std::size_t variable_bytes =
            static_cast<std::size_t>(record.path_bytes) + static_cast<std::size_t>(record.error_bytes);
        if (variable_bytes > buffer.bytes_used - offset) {
            throw std::runtime_error("diff result record variable bytes are truncated");
        }
        const auto* path = reinterpret_cast<const char*>(buffer.bytes.data() + offset);
        const auto* error = path + record.path_bytes;
        FolderDiffSummary summary;
        summary.rel_path = std::string_view(path, record.path_bytes);
        summary.error = std::string_view(error, record.error_bytes);
        summary.source_scan_started_unix_ns = record.source_scan_started_unix_ns;
        summary.source_scan_finished_unix_ns = record.source_scan_finished_unix_ns;
        summary.target_scan_started_unix_ns = record.target_scan_started_unix_ns;
        summary.target_scan_finished_unix_ns = record.target_scan_finished_unix_ns;
        summary.result_sent_unix_ns = record.result_sent_unix_ns;
        summary.source_file_count = record.source_file_count;
        summary.target_file_count = record.target_file_count;
        summary.source_folder_count = record.source_folder_count;
        summary.target_folder_count = record.target_folder_count;
        summary.files_same = record.files_same;
        summary.files_changed = record.files_changed;
        summary.files_new = record.files_new;
        summary.files_target_only = record.files_target_only;
        summary.files_failed = record.files_failed;
        summary.source_logical_size_bytes = record.source_logical_size_bytes;
        summary.target_logical_size_bytes = record.target_logical_size_bytes;
        summary.same_logical_size_bytes = record.same_logical_size_bytes;
        summary.changed_logical_size_bytes = record.changed_logical_size_bytes;
        summary.new_logical_size_bytes = record.new_logical_size_bytes;
        summary.target_only_logical_size_bytes = record.target_only_logical_size_bytes;
        summary.bytes_planned = record.bytes_planned;
        summary.status = record.status;
        visitor(summary);
        offset += variable_bytes;
        ++seen;
    }
    if (seen != header.record_count || seen != buffer.record_count) {
        throw std::runtime_error("diff result record count mismatch");
    }
}

}  // namespace hypersync
