#include "core/metadata_buffer_codec.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>

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

struct EncodedFolderBatchHeader {
    std::uint32_t magic = 0;
    std::uint16_t version = 0;
    std::uint16_t flags = 0;
    std::uint32_t folder_path_bytes = 0;
    std::uint32_t child_record_count = 0;
    std::uint64_t folder_mtime = 0;
    std::uint32_t folder_mode = 0;
    std::uint32_t folder_uid = 0;
    std::uint32_t folder_gid = 0;
    std::uint64_t flat_file_count = 0;
    std::uint64_t flat_logical_size_bytes = 0;
};

struct EncodedFolderBatchChildHeader {
    std::uint32_t kind = 0;
    std::uint32_t name_bytes = 0;
    std::uint64_t size = 0;
    std::uint64_t mtime = 0;
    std::uint32_t mode = 0;
    std::uint32_t uid = 0;
    std::uint32_t gid = 0;
    std::uint32_t reserved = 0;
    std::uint64_t flat_file_count = 0;
    std::uint64_t flat_logical_size_bytes = 0;
};

inline constexpr std::uint32_t kFolderBatchMagic = 0x31424648U;  // "HFB1" little-endian.
inline constexpr std::uint16_t kFolderBatchVersion = 1U;
inline constexpr std::uint16_t kFolderBatchIncludesFolderRecord = 1U << 0U;

static_assert(sizeof(EncodedMetadataRecordHeader) <= kMetadataBufferBytes);
static_assert(sizeof(EncodedBatchRecordHeader) <= kMetadataBatchBufferBytes);
static_assert(sizeof(EncodedFolderBatchHeader) <= kMetadataBatchBufferBytes);
static_assert(sizeof(EncodedFolderBatchChildHeader) <= kMetadataBatchBufferBytes);

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

EncodedFolderBatchHeader read_folder_batch_header(const MetadataBatchBuffer& buffer) {
    if (buffer.bytes_used < sizeof(EncodedFolderBatchHeader)) {
        throw std::runtime_error("folder metadata batch header is truncated");
    }
    EncodedFolderBatchHeader header {};
    std::memcpy(&header, buffer.bytes.data(), sizeof(header));
    if (header.magic != kFolderBatchMagic || header.version != kFolderBatchVersion) {
        throw std::runtime_error("folder metadata batch header is invalid");
    }
    if (header.folder_path_bytes > buffer.bytes_used - sizeof(EncodedFolderBatchHeader)) {
        throw std::runtime_error("folder metadata batch folder path is truncated");
    }
    return header;
}

void write_folder_batch_header(MetadataBatchBuffer& buffer, const EncodedFolderBatchHeader& header) {
    std::memcpy(buffer.bytes.data(), &header, sizeof(header));
}

void increment_folder_batch_child_count(MetadataBatchBuffer& buffer) {
    EncodedFolderBatchHeader header = read_folder_batch_header(buffer);
    ++header.child_record_count;
    write_folder_batch_header(buffer, header);
}

bool is_folder_metadata_batch(const MetadataBatchBuffer& buffer) {
    if (buffer.bytes_used < sizeof(std::uint32_t)) {
        return false;
    }
    std::uint32_t magic = 0;
    std::memcpy(&magic, buffer.bytes.data(), sizeof(magic));
    return magic == kFolderBatchMagic;
}

std::string_view folder_batch_path_view(const MetadataBatchBuffer& buffer,
                                        const EncodedFolderBatchHeader& header) {
    const auto* path = reinterpret_cast<const char*>(buffer.bytes.data() + sizeof(EncodedFolderBatchHeader));
    return std::string_view(path, header.folder_path_bytes);
}

std::string join_child_path(std::string_view folder_path, std::string_view child_name) {
    if (folder_path.empty() || folder_path == ".") {
        return std::string(child_name);
    }
    std::string path;
    path.reserve(folder_path.size() + 1U + child_name.size());
    path.append(folder_path);
    path.push_back('/');
    path.append(child_name);
    return path;
}

std::string_view child_name_view(std::string_view folder_path, std::string_view rel_path) {
    if (folder_path.empty() || folder_path == ".") {
        return rel_path;
    }
    if (rel_path.size() > folder_path.size() &&
        rel_path.compare(0U, folder_path.size(), folder_path) == 0 &&
        rel_path[folder_path.size()] == '/') {
        return rel_path.substr(folder_path.size() + 1U);
    }
    return rel_path;
}

bool append_folder_child_record(MetadataBatchBuffer& buffer,
                                MetadataBufferRecordKind kind,
                                const EncodedMetadataRecordHeader& record,
                                std::string_view name) {
    if (!is_folder_metadata_batch(buffer)) {
        return false;
    }
    if (name.empty() || name.size() > std::numeric_limits<std::uint32_t>::max()) {
        return false;
    }
    const std::size_t needed = sizeof(EncodedFolderBatchChildHeader) + name.size();
    if (needed > kMetadataBatchBufferBytes - buffer.bytes_used) {
        return false;
    }

    EncodedFolderBatchChildHeader child {};
    child.kind = static_cast<std::uint32_t>(kind);
    child.name_bytes = static_cast<std::uint32_t>(name.size());
    child.size = record.size;
    child.mtime = record.mtime;
    child.mode = record.mode;
    child.uid = record.uid;
    child.gid = record.gid;
    child.flat_file_count = record.flat_file_count;
    child.flat_logical_size_bytes = record.flat_logical_size_bytes;

    auto* cursor = buffer.bytes.data() + buffer.bytes_used;
    std::memcpy(cursor, &child, sizeof(child));
    cursor += sizeof(child);
    std::memcpy(cursor, name.data(), name.size());
    buffer.bytes_used += static_cast<std::uint32_t>(needed);
    increment_folder_batch_child_count(buffer);
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

void decode_folder_metadata_batch(const MetadataBatchBuffer& buffer,
                                  std::vector<FileSpec>& files,
                                  std::vector<MetadataFolderRecord>& folders) {
    const EncodedFolderBatchHeader header = read_folder_batch_header(buffer);
    const std::string_view folder_path = folder_batch_path_view(buffer, header);
    if ((header.flags & kFolderBatchIncludesFolderRecord) != 0U) {
        MetadataFolderRecord folder;
        folder.spec.rel_path.assign(folder_path);
        folder.spec.mtime = header.folder_mtime;
        folder.spec.mode = header.folder_mode;
        folder.spec.uid = header.folder_uid;
        folder.spec.gid = header.folder_gid;
        folder.flat_file_count = static_cast<std::size_t>(header.flat_file_count);
        folder.flat_logical_size_bytes = header.flat_logical_size_bytes;
        folders.push_back(std::move(folder));
    }

    std::size_t offset = sizeof(EncodedFolderBatchHeader) + header.folder_path_bytes;
    std::uint32_t children_seen = 0;
    while (offset < buffer.bytes_used) {
        if (buffer.bytes_used - offset < sizeof(EncodedFolderBatchChildHeader)) {
            throw std::runtime_error("folder metadata batch child header is truncated");
        }
        EncodedFolderBatchChildHeader child {};
        std::memcpy(&child, buffer.bytes.data() + offset, sizeof(child));
        offset += sizeof(child);
        if (child.name_bytes > buffer.bytes_used - offset) {
            throw std::runtime_error("folder metadata batch child name is truncated");
        }
        const auto* name_data = reinterpret_cast<const char*>(buffer.bytes.data() + offset);
        const std::string_view name(name_data, child.name_bytes);
        if (child.kind == static_cast<std::uint32_t>(MetadataBufferRecordKind::file)) {
            FileSpec file;
            file.rel_path = join_child_path(folder_path, name);
            file.declared_size = child.size;
            file.mtime = child.mtime;
            file.mode = child.mode;
            file.uid = child.uid;
            file.gid = child.gid;
            files.push_back(std::move(file));
        } else if (child.kind == static_cast<std::uint32_t>(MetadataBufferRecordKind::folder)) {
            MetadataFolderRecord folder;
            folder.spec.rel_path = join_child_path(folder_path, name);
            folder.spec.mtime = child.mtime;
            folder.spec.mode = child.mode;
            folder.spec.uid = child.uid;
            folder.spec.gid = child.gid;
            folder.flat_file_count = static_cast<std::size_t>(child.flat_file_count);
            folder.flat_logical_size_bytes = child.flat_logical_size_bytes;
            folders.push_back(std::move(folder));
        } else {
            throw std::runtime_error("folder metadata batch child kind is unsupported");
        }
        offset += child.name_bytes;
        ++children_seen;
    }
    if (children_seen != header.child_record_count || buffer.record_count != children_seen + ((header.flags & kFolderBatchIncludesFolderRecord) != 0U ? 1U : 0U)) {
        throw std::runtime_error("folder metadata batch record count mismatch");
    }
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
    buffer.bytes_used = 0;
    buffer.record_count = 0;
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

bool reset_folder_metadata_batch(MetadataBatchBuffer& buffer,
                                 const MetadataFolderRecord& folder,
                                 bool include_folder_record) {
    buffer.bytes_used = 0;
    buffer.record_count = 0;
    if (folder.spec.rel_path.size() > kMetadataBatchBufferBytes - sizeof(EncodedFolderBatchHeader) ||
        folder.spec.rel_path.size() > std::numeric_limits<std::uint32_t>::max()) {
        return false;
    }

    EncodedFolderBatchHeader header {};
    header.magic = kFolderBatchMagic;
    header.version = kFolderBatchVersion;
    header.flags = include_folder_record ? kFolderBatchIncludesFolderRecord : 0U;
    header.folder_path_bytes = static_cast<std::uint32_t>(folder.spec.rel_path.size());
    header.folder_mtime = folder.spec.mtime;
    header.folder_mode = folder.spec.mode;
    header.folder_uid = folder.spec.uid;
    header.folder_gid = folder.spec.gid;
    header.flat_file_count = folder.flat_file_count;
    header.flat_logical_size_bytes = folder.flat_logical_size_bytes;
    std::memcpy(buffer.bytes.data(), &header, sizeof(header));
    std::memcpy(buffer.bytes.data() + sizeof(header), folder.spec.rel_path.data(), folder.spec.rel_path.size());
    buffer.bytes_used = static_cast<std::uint32_t>(sizeof(header) + folder.spec.rel_path.size());
    buffer.record_count = include_folder_record ? 1U : 0U;
    return true;
}

bool append_folder_metadata_batch_file(MetadataBatchBuffer& buffer, const FileSpec& file) {
    const EncodedFolderBatchHeader header = read_folder_batch_header(buffer);
    EncodedMetadataRecordHeader record {};
    record.size = file.declared_size != 0 ? file.declared_size : file.content.size();
    record.mtime = file.mtime;
    record.mode = file.mode;
    record.uid = file.uid;
    record.gid = file.gid;
    return append_folder_child_record(buffer,
                                      MetadataBufferRecordKind::file,
                                      record,
                                      child_name_view(folder_batch_path_view(buffer, header), file.rel_path));
}

bool append_folder_metadata_batch_file(MetadataBatchBuffer& buffer, const GeneratedFileMetadataView& file) {
    const EncodedFolderBatchHeader header = read_folder_batch_header(buffer);
    EncodedMetadataRecordHeader record {};
    record.size = file.declared_size;
    record.mtime = file.mtime;
    record.mode = file.mode;
    record.uid = file.uid;
    record.gid = file.gid;
    return append_folder_child_record(buffer,
                                      MetadataBufferRecordKind::file,
                                      record,
                                      child_name_view(folder_batch_path_view(buffer, header), file.rel_path));
}

bool append_folder_metadata_batch_folder(MetadataBatchBuffer& buffer, const MetadataFolderRecord& folder) {
    const EncodedFolderBatchHeader header = read_folder_batch_header(buffer);
    EncodedMetadataRecordHeader record {};
    record.mtime = folder.spec.mtime;
    record.mode = folder.spec.mode;
    record.uid = folder.spec.uid;
    record.gid = folder.spec.gid;
    record.flat_file_count = folder.flat_file_count;
    record.flat_logical_size_bytes = folder.flat_logical_size_bytes;
    return append_folder_child_record(buffer,
                                      MetadataBufferRecordKind::folder,
                                      record,
                                      child_name_view(folder_batch_path_view(buffer, header), folder.spec.rel_path));
}

bool append_folder_metadata_batch_folder(MetadataBatchBuffer& buffer, const GeneratedFolderMetadataView& folder) {
    const EncodedFolderBatchHeader header = read_folder_batch_header(buffer);
    EncodedMetadataRecordHeader record {};
    record.mtime = folder.mtime;
    record.mode = folder.mode;
    record.uid = folder.uid;
    record.gid = folder.gid;
    record.flat_file_count = folder.flat_file_count;
    record.flat_logical_size_bytes = folder.flat_logical_size_bytes;
    return append_folder_child_record(buffer,
                                      MetadataBufferRecordKind::folder,
                                      record,
                                      child_name_view(folder_batch_path_view(buffer, header), folder.rel_path));
}

void decode_metadata_batch(const MetadataBatchBuffer& buffer,
                           std::vector<FileSpec>& files,
                           std::vector<MetadataFolderRecord>& folders) {
    if (is_folder_metadata_batch(buffer)) {
        decode_folder_metadata_batch(buffer, files, folders);
        return;
    }

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
