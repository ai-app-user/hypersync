#ifndef HYPERSYNC_CORE_DATA_BUFFER_CODEC_HPP
#define HYPERSYNC_CORE_DATA_BUFFER_CODEC_HPP

// Helpers for interpreting a generic DataBuffer payload.
//
// DataBuffer remains an opaque fixed-size buffer at the piper/job layer. This
// codec describes a Hypersync-specific payload layout for batches of small file
// contents. It is intended for the NFS data reader to fill directly at the
// libnfs -> owned-buffer boundary, so downstream jobs can hash, send, receive,
// discard, or write the same buffer without copying file bytes again.

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "common/types.hpp"
#include "core/pipeline_buffers.hpp"

namespace hypersync {

struct PackedSmallFileMeta {
    std::uint64_t file_id = 0;
    std::uint64_t folder_hash = 0;
    std::uint64_t file_size = 0;
    std::uint64_t mtime = 0;
    std::uint32_t mode = 0;
    std::uint32_t uid = 0;
    std::uint32_t gid = 0;
    std::string_view rel_path;
};

struct PackedSmallFileView {
    std::uint64_t file_id = 0;
    std::uint64_t folder_hash = 0;
    std::uint64_t file_size = 0;
    std::uint64_t mtime = 0;
    std::uint32_t mode = 0;
    std::uint32_t uid = 0;
    std::uint32_t gid = 0;
    std::string_view rel_path;
    std::string_view data;
};

struct PackedSmallFileAppend {
    std::byte* data = nullptr;
    std::size_t data_capacity = 0;
    std::uint32_t count_after_commit = 0;
    std::uint32_t bytes_used_after_commit = 0;
    std::uint64_t payload_bytes_after_commit = 0;
};

// Mark the buffer as an empty packed-small-file batch.
void reset_packed_small_file_buffer(DataBuffer& buffer) noexcept;

// Reserve the final payload location for one file inside this buffer. The caller
// may read external data directly into append.data and then commit. Until commit
// the buffer header still describes the previous valid batch, so a failed read
// can simply abandon the reservation without corrupting already packed files.
[[nodiscard]] bool prepare_packed_small_file_append(DataBuffer& buffer,
                                                   const PackedSmallFileMeta& meta,
                                                   std::size_t data_len,
                                                   PackedSmallFileAppend& append) noexcept;

void commit_packed_small_file_append(DataBuffer& buffer,
                                     const PackedSmallFileAppend& append) noexcept;

// Append one small file to an owned batch buffer. The payload bytes are copied
// into the buffer by the caller-facing boundary operation; downstream jobs must
// only transfer ownership of this filled buffer.
[[nodiscard]] bool append_packed_small_file(DataBuffer& buffer,
                                            const PackedSmallFileMeta& meta,
                                            std::string_view data) noexcept;

// Convenience helper for tests and legacy call sites. Production readers should
// pass explicit hashes in PackedSmallFileMeta to avoid path normalization work on
// the hot data path.
[[nodiscard]] bool append_packed_small_file(DataBuffer& buffer,
                                            const FileSpec& file,
                                            std::string_view data);

[[nodiscard]] bool is_packed_small_file_buffer(const DataBuffer& buffer) noexcept;
[[nodiscard]] std::uint32_t packed_small_file_count(const DataBuffer& buffer) noexcept;
[[nodiscard]] std::uint64_t packed_small_file_payload_bytes(const DataBuffer& buffer) noexcept;

namespace data_buffer_codec_detail {

inline constexpr std::uint32_t kPackedSmallFileMagic = 0x4853504bU;  // "HSPK"
inline constexpr std::uint16_t kPackedSmallFileVersion = 1U;
inline constexpr std::size_t kPackedSmallFileHeaderBytes = 24U;
inline constexpr std::size_t kPackedSmallFileEntryHeaderBytes = 64U;

[[nodiscard]] inline std::uint16_t read_u16(const std::byte* bytes) noexcept {
    return static_cast<std::uint16_t>(static_cast<std::uint16_t>(bytes[0]) |
                                      (static_cast<std::uint16_t>(bytes[1]) << 8U));
}

[[nodiscard]] inline std::uint32_t read_u32(const std::byte* bytes) noexcept {
    return static_cast<std::uint32_t>(static_cast<std::uint32_t>(bytes[0]) |
                                      (static_cast<std::uint32_t>(bytes[1]) << 8U) |
                                      (static_cast<std::uint32_t>(bytes[2]) << 16U) |
                                      (static_cast<std::uint32_t>(bytes[3]) << 24U));
}

[[nodiscard]] inline std::uint64_t read_u64(const std::byte* bytes) noexcept {
    std::uint64_t value = 0;
    for (std::size_t index = 0; index < 8U; ++index) {
        value |= static_cast<std::uint64_t>(bytes[index]) << (index * 8U);
    }
    return value;
}

}  // namespace data_buffer_codec_detail

// Visit every file entry in a packed buffer without allocation. Returns false if
// the buffer is malformed or not a packed-small-file payload.
template <typename Visitor>
[[nodiscard]] bool visit_packed_small_files(const DataBuffer& buffer, Visitor&& visitor) {
    namespace detail = data_buffer_codec_detail;

    if (!is_packed_small_file_buffer(buffer)) {
        return false;
    }

    const std::byte* const bytes = buffer.bytes.data();
    const std::size_t bytes_used = static_cast<std::size_t>(detail::read_u32(bytes + 8U));
    if (bytes_used > buffer.bytes.size() || bytes_used < detail::kPackedSmallFileHeaderBytes) {
        return false;
    }

    std::size_t offset = detail::kPackedSmallFileHeaderBytes;
    const std::uint32_t count = packed_small_file_count(buffer);
    for (std::uint32_t index = 0; index < count; ++index) {
        if (offset + detail::kPackedSmallFileEntryHeaderBytes > bytes_used) {
            return false;
        }

        PackedSmallFileView view;
        view.file_id = detail::read_u64(bytes + offset);
        view.folder_hash = detail::read_u64(bytes + offset + 8U);
        const std::uint64_t data_len = detail::read_u64(bytes + offset + 16U);
        view.file_size = detail::read_u64(bytes + offset + 24U);
        view.mtime = detail::read_u64(bytes + offset + 32U);
        view.mode = detail::read_u32(bytes + offset + 40U);
        view.uid = detail::read_u32(bytes + offset + 44U);
        view.gid = detail::read_u32(bytes + offset + 48U);
        const std::uint32_t path_len = detail::read_u32(bytes + offset + 52U);
        offset += detail::kPackedSmallFileEntryHeaderBytes;

        if (path_len > kDataBufRelPathBytes || data_len > buffer.bytes.size()) {
            return false;
        }
        if (offset + path_len > bytes_used) {
            return false;
        }
        view.rel_path = std::string_view(reinterpret_cast<const char*>(bytes + offset), path_len);
        offset += path_len;

        if (offset + static_cast<std::size_t>(data_len) > bytes_used) {
            return false;
        }
        view.data = std::string_view(reinterpret_cast<const char*>(bytes + offset),
                                     static_cast<std::size_t>(data_len));
        offset += static_cast<std::size_t>(data_len);

        visitor(view);
    }

    return offset == bytes_used;
}

}  // namespace hypersync

#endif
