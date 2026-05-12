#include "core/metadata_buffer_codec.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <stdexcept>

namespace hypersync {
namespace {

struct EncodedMetadataRecordHeader {
    std::uint64_t size = 0;
    std::uint64_t mtime = 0;
    std::uint32_t mode = 0;
    std::uint32_t uid = 0;
    std::uint32_t gid = 0;
    std::uint64_t flat_file_count = 0;
    std::uint64_t flat_logical_size_bytes = 0;
    std::uint32_t path_bytes = 0;
};

struct EncodedBatchRecordHeader {
    std::uint32_t kind = 0;
    std::uint32_t bytes = 0;
};

static_assert(sizeof(EncodedMetadataRecordHeader) <= kMetadataBufferBytes);
static_assert(sizeof(EncodedBatchRecordHeader) <= kMetadataBatchBufferBytes);

bool set_path(MetadataBuffer& buffer, EncodedMetadataRecordHeader& record, std::string_view path) {
    if (path.size() > kMetadataBufferBytes - sizeof(EncodedMetadataRecordHeader)) {
        return false;
    }
    record.path_bytes = static_cast<std::uint32_t>(path.size());
    std::memcpy(buffer.bytes.data() + sizeof(EncodedMetadataRecordHeader), path.data(), path.size());
    buffer.bytes_used = static_cast<std::uint32_t>(sizeof(EncodedMetadataRecordHeader) + path.size());
    return true;
}

std::string decode_path(const MetadataBuffer& buffer, const EncodedMetadataRecordHeader& record) {
    if (record.path_bytes > kMetadataBufferBytes - sizeof(EncodedMetadataRecordHeader) ||
        buffer.bytes_used != sizeof(EncodedMetadataRecordHeader) + record.path_bytes) {
        throw std::runtime_error("encoded metadata path length is invalid");
    }
    const auto* path = reinterpret_cast<const char*>(buffer.bytes.data() + sizeof(EncodedMetadataRecordHeader));
    return std::string(path, path + record.path_bytes);
}

const EncodedMetadataRecordHeader& encoded_record(const MetadataBuffer& buffer) {
    if (buffer.bytes_used < sizeof(EncodedMetadataRecordHeader) || buffer.bytes_used > kMetadataBufferBytes) {
        throw std::runtime_error("metadata buffer does not contain encoded metadata record");
    }
    return *reinterpret_cast<const EncodedMetadataRecordHeader*>(buffer.bytes.data());
}

EncodedMetadataRecordHeader& encoded_record(MetadataBuffer& buffer) {
    return *reinterpret_cast<EncodedMetadataRecordHeader*>(buffer.bytes.data());
}

std::size_t encoded_record_bytes(std::string_view path) {
    return sizeof(EncodedMetadataRecordHeader) + path.size();
}

bool append_encoded_record(MetadataBatchBuffer& buffer,
                           MetadataBufferRecordKind kind,
                           const EncodedMetadataRecordHeader& record,
                           std::string_view path) {
    const std::size_t record_bytes = encoded_record_bytes(path);
    if (record_bytes > std::numeric_limits<std::uint32_t>::max()) {
        return false;
    }
    const std::size_t needed = sizeof(EncodedBatchRecordHeader) + record_bytes;
    if (needed > kMetadataBatchBufferBytes - buffer.bytes_used) {
        return false;
    }

    auto* cursor = buffer.bytes.data() + buffer.bytes_used;
    EncodedBatchRecordHeader header;
    header.kind = static_cast<std::uint32_t>(kind);
    header.bytes = static_cast<std::uint32_t>(record_bytes);
    std::memcpy(cursor, &header, sizeof(header));
    cursor += sizeof(header);
    std::memcpy(cursor, &record, sizeof(record));
    cursor += sizeof(record);
    std::memcpy(cursor, path.data(), path.size());
    buffer.bytes_used += static_cast<std::uint32_t>(needed);
    ++buffer.record_count;
    return true;
}

FileSpec decode_file_payload(const std::byte* payload, std::uint32_t payload_bytes) {
    if (payload_bytes < sizeof(EncodedMetadataRecordHeader)) {
        throw std::runtime_error("metadata batch file payload is truncated");
    }
    EncodedMetadataRecordHeader record {};
    std::memcpy(&record, payload, sizeof(record));
    const std::uint32_t path_bytes = payload_bytes - static_cast<std::uint32_t>(sizeof(record));
    if (record.path_bytes != path_bytes) {
        throw std::runtime_error("metadata batch file path length is invalid");
    }
    const auto* path = reinterpret_cast<const char*>(payload + sizeof(record));
    FileSpec file;
    file.rel_path.assign(path, path + path_bytes);
    file.declared_size = record.size;
    file.mtime = record.mtime;
    file.mode = record.mode;
    file.uid = record.uid;
    file.gid = record.gid;
    return file;
}

MetadataFolderRecord decode_folder_payload(const std::byte* payload, std::uint32_t payload_bytes) {
    if (payload_bytes < sizeof(EncodedMetadataRecordHeader)) {
        throw std::runtime_error("metadata batch folder payload is truncated");
    }
    EncodedMetadataRecordHeader record {};
    std::memcpy(&record, payload, sizeof(record));
    const std::uint32_t path_bytes = payload_bytes - static_cast<std::uint32_t>(sizeof(record));
    if (record.path_bytes != path_bytes) {
        throw std::runtime_error("metadata batch folder path length is invalid");
    }
    const auto* path = reinterpret_cast<const char*>(payload + sizeof(record));
    MetadataFolderRecord folder;
    folder.spec.rel_path.assign(path, path + path_bytes);
    folder.spec.mtime = record.mtime;
    folder.spec.mode = record.mode;
    folder.spec.uid = record.uid;
    folder.spec.gid = record.gid;
    folder.flat_file_count = static_cast<std::size_t>(record.flat_file_count);
    folder.flat_logical_size_bytes = record.flat_logical_size_bytes;
    return folder;
}

}  // namespace

bool encode_metadata_file_record(MetadataBuffer& buffer, const FileSpec& file) {
    buffer = MetadataBuffer {};
    buffer.record_kind = MetadataBufferRecordKind::file;
    EncodedMetadataRecordHeader& record = encoded_record(buffer);
    if (!set_path(buffer, record, file.rel_path)) {
        return false;
    }
    record.size = file.declared_size != 0 ? file.declared_size : file.content.size();
    record.mtime = file.mtime;
    record.mode = file.mode;
    record.uid = file.uid;
    record.gid = file.gid;
    return true;
}

bool encode_metadata_folder_record(MetadataBuffer& buffer, const MetadataFolderRecord& folder) {
    buffer = MetadataBuffer {};
    buffer.record_kind = MetadataBufferRecordKind::folder;
    EncodedMetadataRecordHeader& record = encoded_record(buffer);
    if (!set_path(buffer, record, folder.spec.rel_path)) {
        return false;
    }
    record.mtime = folder.spec.mtime;
    record.mode = folder.spec.mode;
    record.uid = folder.spec.uid;
    record.gid = folder.spec.gid;
    record.flat_file_count = folder.flat_file_count;
    record.flat_logical_size_bytes = folder.flat_logical_size_bytes;
    return true;
}

FileSpec decode_metadata_file_record(const MetadataBuffer& buffer) {
    if (buffer.record_kind != MetadataBufferRecordKind::file) {
        throw std::runtime_error("metadata buffer is not a file record");
    }
    const EncodedMetadataRecordHeader& record = encoded_record(buffer);
    FileSpec file;
    file.rel_path = decode_path(buffer, record);
    file.declared_size = record.size;
    file.mtime = record.mtime;
    file.mode = record.mode;
    file.uid = record.uid;
    file.gid = record.gid;
    return file;
}

MetadataFolderRecord decode_metadata_folder_record(const MetadataBuffer& buffer) {
    if (buffer.record_kind != MetadataBufferRecordKind::folder) {
        throw std::runtime_error("metadata buffer is not a folder record");
    }
    const EncodedMetadataRecordHeader& record = encoded_record(buffer);
    MetadataFolderRecord folder;
    folder.spec.rel_path = decode_path(buffer, record);
    folder.spec.mtime = record.mtime;
    folder.spec.mode = record.mode;
    folder.spec.uid = record.uid;
    folder.spec.gid = record.gid;
    folder.flat_file_count = static_cast<std::size_t>(record.flat_file_count);
    folder.flat_logical_size_bytes = record.flat_logical_size_bytes;
    return folder;
}

void reset_metadata_batch(MetadataBatchBuffer& buffer) {
    buffer = MetadataBatchBuffer {};
}

bool append_metadata_batch_file(MetadataBatchBuffer& buffer, const FileSpec& file) {
    EncodedMetadataRecordHeader record {};
    record.path_bytes = static_cast<std::uint32_t>(file.rel_path.size());
    record.size = file.declared_size != 0 ? file.declared_size : file.content.size();
    record.mtime = file.mtime;
    record.mode = file.mode;
    record.uid = file.uid;
    record.gid = file.gid;
    return append_encoded_record(buffer, MetadataBufferRecordKind::file, record, file.rel_path);
}

bool append_metadata_batch_folder(MetadataBatchBuffer& buffer, const MetadataFolderRecord& folder) {
    EncodedMetadataRecordHeader record {};
    record.path_bytes = static_cast<std::uint32_t>(folder.spec.rel_path.size());
    record.mtime = folder.spec.mtime;
    record.mode = folder.spec.mode;
    record.uid = folder.spec.uid;
    record.gid = folder.spec.gid;
    record.flat_file_count = folder.flat_file_count;
    record.flat_logical_size_bytes = folder.flat_logical_size_bytes;
    return append_encoded_record(buffer, MetadataBufferRecordKind::folder, record, folder.spec.rel_path);
}

bool append_metadata_batch_file(MetadataBatchBuffer& buffer, const GeneratedFileMetadataView& file) {
    EncodedMetadataRecordHeader record {};
    record.path_bytes = static_cast<std::uint32_t>(file.rel_path.size());
    record.size = file.declared_size;
    record.mtime = file.mtime;
    record.mode = file.mode;
    record.uid = file.uid;
    record.gid = file.gid;
    return append_encoded_record(buffer, MetadataBufferRecordKind::file, record, file.rel_path);
}

bool append_metadata_batch_folder(MetadataBatchBuffer& buffer, const GeneratedFolderMetadataView& folder) {
    EncodedMetadataRecordHeader record {};
    record.path_bytes = static_cast<std::uint32_t>(folder.rel_path.size());
    record.mtime = folder.mtime;
    record.mode = folder.mode;
    record.uid = folder.uid;
    record.gid = folder.gid;
    record.flat_file_count = folder.flat_file_count;
    record.flat_logical_size_bytes = folder.flat_logical_size_bytes;
    return append_encoded_record(buffer, MetadataBufferRecordKind::folder, record, folder.rel_path);
}

void decode_metadata_batch(const MetadataBatchBuffer& buffer,
                           std::vector<FileSpec>& files,
                           std::vector<MetadataFolderRecord>& folders) {
    std::size_t offset = 0;
    std::uint32_t records_seen = 0;
    while (offset < buffer.bytes_used) {
        if (buffer.bytes_used - offset < sizeof(EncodedBatchRecordHeader)) {
            throw std::runtime_error("metadata batch record header is truncated");
        }
        EncodedBatchRecordHeader header {};
        std::memcpy(&header, buffer.bytes.data() + offset, sizeof(header));
        offset += sizeof(header);
        if (header.bytes > buffer.bytes_used - offset) {
            throw std::runtime_error("metadata batch record payload is truncated");
        }
        const auto* payload = buffer.bytes.data() + offset;
        if (header.kind == static_cast<std::uint32_t>(MetadataBufferRecordKind::file)) {
            files.push_back(decode_file_payload(payload, header.bytes));
        } else if (header.kind == static_cast<std::uint32_t>(MetadataBufferRecordKind::folder)) {
            folders.push_back(decode_folder_payload(payload, header.bytes));
        } else {
            throw std::runtime_error("metadata batch record kind is unsupported");
        }
        offset += header.bytes;
        ++records_seen;
    }
    if (records_seen != buffer.record_count) {
        throw std::runtime_error("metadata batch record count mismatch");
    }
}

}  // namespace hypersync
