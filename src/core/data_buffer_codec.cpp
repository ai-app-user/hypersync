#include "core/data_buffer_codec.hpp"

#include <algorithm>
#include <cstring>

#include "common/hash_utils.hpp"
#include "common/path_utils.hpp"
#include "common/records.hpp"

namespace hypersync {
namespace {

namespace detail = data_buffer_codec_detail;

void write_u16(std::byte* bytes, std::uint16_t value) noexcept {
    bytes[0] = static_cast<std::byte>(value & 0xffU);
    bytes[1] = static_cast<std::byte>((value >> 8U) & 0xffU);
}

void write_u32(std::byte* bytes, std::uint32_t value) noexcept {
    for (std::size_t index = 0; index < 4U; ++index) {
        bytes[index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
    }
}

void write_u64(std::byte* bytes, std::uint64_t value) noexcept {
    for (std::size_t index = 0; index < 8U; ++index) {
        bytes[index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
    }
}

void write_header(DataBuffer& buffer,
                  std::uint32_t count,
                  std::uint32_t bytes_used,
                  std::uint64_t payload_bytes) noexcept {
    std::byte* const bytes = buffer.bytes.data();
    write_u32(bytes, detail::kPackedSmallFileMagic);
    write_u16(bytes + 4U, detail::kPackedSmallFileVersion);
    write_u16(bytes + 6U, 0U);
    write_u32(bytes + 8U, bytes_used);
    write_u32(bytes + 12U, count);
    write_u64(bytes + 16U, payload_bytes);
}

}  // namespace

void reset_packed_small_file_buffer(DataBuffer& buffer) noexcept {
    buffer.trailer = DataBufTrailer {};
    buffer.trailer.flags = kFlagPackedSmallFiles | kFlagLastChunk;
    buffer.trailer.data_len = detail::kPackedSmallFileHeaderBytes;
    write_header(buffer, 0U, static_cast<std::uint32_t>(detail::kPackedSmallFileHeaderBytes), 0U);
}

bool append_packed_small_file(DataBuffer& buffer,
                              const PackedSmallFileMeta& meta,
                              std::string_view data) noexcept {
    PackedSmallFileAppend append;
    if (!prepare_packed_small_file_append(buffer, meta, data.size(), append)) {
        return false;
    }
    if (!data.empty()) {
        std::memcpy(append.data, data.data(), data.size());
    }
    commit_packed_small_file_append(buffer, append);
    return true;
}

bool prepare_packed_small_file_append(DataBuffer& buffer,
                                      const PackedSmallFileMeta& meta,
                                      std::size_t data_len,
                                      PackedSmallFileAppend& append) noexcept {
    if (!is_packed_small_file_buffer(buffer)) {
        return false;
    }

    const std::size_t bytes_used = static_cast<std::size_t>(detail::read_u32(buffer.bytes.data() + 8U));
    const std::uint32_t count = packed_small_file_count(buffer);
    const std::uint64_t payload_bytes = packed_small_file_payload_bytes(buffer);
    const std::size_t path_len = meta.rel_path.size();
    const std::size_t needed = detail::kPackedSmallFileEntryHeaderBytes + path_len + data_len;
    if (path_len > kDataBufRelPathBytes || needed > buffer.bytes.size() || bytes_used + needed > buffer.bytes.size()) {
        return false;
    }

    std::byte* const bytes = buffer.bytes.data();
    std::size_t offset = bytes_used;
    write_u64(bytes + offset, meta.file_id);
    write_u64(bytes + offset + 8U, meta.folder_hash);
    write_u64(bytes + offset + 16U, data_len);
    write_u64(bytes + offset + 24U, meta.file_size);
    write_u64(bytes + offset + 32U, meta.mtime);
    write_u32(bytes + offset + 40U, meta.mode);
    write_u32(bytes + offset + 44U, meta.uid);
    write_u32(bytes + offset + 48U, meta.gid);
    write_u32(bytes + offset + 52U, static_cast<std::uint32_t>(path_len));
    write_u64(bytes + offset + 56U, 0U);
    offset += detail::kPackedSmallFileEntryHeaderBytes;

    if (path_len != 0U) {
        std::memcpy(bytes + offset, meta.rel_path.data(), path_len);
        offset += path_len;
    }

    append.data = bytes + offset;
    append.data_capacity = data_len;
    append.count_after_commit = count + 1U;
    append.bytes_used_after_commit = static_cast<std::uint32_t>(offset + data_len);
    append.payload_bytes_after_commit = payload_bytes + data_len;
    return true;
}

void commit_packed_small_file_append(DataBuffer& buffer,
                                     const PackedSmallFileAppend& append) noexcept {
    write_header(buffer,
                 append.count_after_commit,
                 append.bytes_used_after_commit,
                 append.payload_bytes_after_commit);
    buffer.trailer.data_len = append.bytes_used_after_commit;
    buffer.trailer.file_size = append.payload_bytes_after_commit;
    buffer.trailer.flags = kFlagPackedSmallFiles | kFlagLastChunk;
}

bool append_packed_small_file(DataBuffer& buffer, const FileSpec& file, std::string_view data) {
    const RecBuf record = make_recbuf(file);
    PackedSmallFileMeta meta;
    meta.file_id = record.own_hash;
    meta.folder_hash = record.folder_hash;
    meta.file_size = file.declared_size != 0U ? file.declared_size : data.size();
    meta.mtime = file.mtime;
    meta.mode = file.mode;
    meta.uid = file.uid;
    meta.gid = file.gid;
    meta.rel_path = record.rel_path.view();
    return append_packed_small_file(buffer, meta, data);
}

bool is_packed_small_file_buffer(const DataBuffer& buffer) noexcept {
    const std::byte* const bytes = buffer.bytes.data();
    return (buffer.trailer.flags & kFlagPackedSmallFiles) != 0U &&
           detail::read_u32(bytes) == detail::kPackedSmallFileMagic &&
           detail::read_u16(bytes + 4U) == detail::kPackedSmallFileVersion;
}

std::uint32_t packed_small_file_count(const DataBuffer& buffer) noexcept {
    if (!is_packed_small_file_buffer(buffer)) {
        return 0U;
    }
    return detail::read_u32(buffer.bytes.data() + 12U);
}

std::uint64_t packed_small_file_payload_bytes(const DataBuffer& buffer) noexcept {
    if (!is_packed_small_file_buffer(buffer)) {
        return 0U;
    }
    return detail::read_u64(buffer.bytes.data() + 16U);
}

}  // namespace hypersync
