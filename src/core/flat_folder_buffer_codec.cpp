#include "core/flat_folder_buffer_codec.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>

#include "common/hash_utils.hpp"
#include "common/path_utils.hpp"

namespace hypersync {
namespace {

struct EncodedFlatFolderHeader {
    std::uint32_t magic = 0;
    std::uint16_t version = 0;
    std::uint16_t flags = 0;
    std::uint32_t sequence = 0;
    std::uint32_t folder_path_bytes = 0;
    std::uint32_t error_bytes = 0;
    std::uint32_t child_record_count = 0;
    std::uint64_t folder_hash = 0;
    std::uint64_t folder_mtime = 0;
    std::uint32_t folder_mode = 0;
    std::uint32_t folder_uid = 0;
    std::uint32_t folder_gid = 0;
    std::uint32_t reserved = 0;
    std::uint64_t scan_started_unix_ns = 0;
    std::uint64_t scan_finished_unix_ns = 0;
    std::uint64_t total_file_count = 0;
    std::uint64_t total_folder_count = 0;
    std::uint64_t total_logical_size_bytes = 0;
    std::uint64_t metadata_hash = 0;
};

struct EncodedFlatFolderChild {
    std::uint32_t kind = 0;
    std::uint32_t name_bytes = 0;
    std::uint64_t name_hash = 0;
    std::uint64_t metadata_hash = 0;
    std::uint64_t logical_size = 0;
    std::uint64_t mtime = 0;
    std::uint32_t mode = 0;
    std::uint32_t uid = 0;
    std::uint32_t gid = 0;
    std::uint32_t reserved = 0;
    std::uint64_t flat_file_count = 0;
    std::uint64_t flat_logical_size_bytes = 0;
};

inline constexpr std::uint32_t kFlatFolderMagic = 0x32464648U;  // "HFF2" little-endian.
inline constexpr std::uint16_t kFlatFolderVersion = 1U;
inline constexpr std::uint16_t kFlatFolderFinal = 1U << 0U;
inline constexpr std::uint16_t kFlatFolderFailed = 1U << 1U;
inline constexpr std::uint32_t kFlatFolderChildFile = 1U;
inline constexpr std::uint32_t kFlatFolderChildFolder = 2U;
inline constexpr std::uint64_t kMixPrime = 11400714785074694791ULL;

static_assert(sizeof(EncodedFlatFolderHeader) <= kMetadataBatchBufferBytes);
static_assert(sizeof(EncodedFlatFolderChild) <= kMetadataBatchBufferBytes);

[[nodiscard]] std::uint64_t logical_size(const FileSpec& file) {
    return file.declared_size != 0U ? file.declared_size : file.content.size();
}

[[nodiscard]] std::uint64_t mix_u64(std::uint64_t seed, std::uint64_t value) {
    seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
    return seed;
}

[[nodiscard]] std::string_view child_name_view(std::string_view rel_path) {
    while (!rel_path.empty() && rel_path.back() == '/') {
        rel_path.remove_suffix(1U);
    }
    const std::size_t slash = rel_path.find_last_of('/');
    if (slash == std::string_view::npos) {
        return rel_path;
    }
    return rel_path.substr(slash + 1U);
}

[[nodiscard]] std::uint64_t child_name_hash(std::string_view name) {
    return hash64(name);
}

[[nodiscard]] std::uint64_t child_metadata_hash(std::string_view name,
                                                bool is_file,
                                                std::uint64_t size,
                                                std::uint64_t mtime,
                                                std::uint32_t mode,
                                                std::uint32_t uid,
                                                std::uint32_t gid,
                                                std::string_view compare_mode) {
    std::uint64_t hash = mix_u64(child_name_hash(name), is_file ? 1U : 2U);
    hash = mix_u64(hash, size);
    if (compare_mode == "time" || compare_mode == "content" || compare_mode == "metadata") {
        hash = mix_u64(hash, mtime);
    }
    if (compare_mode == "metadata") {
        hash = mix_u64(hash, mode);
        hash = mix_u64(hash, uid);
        hash = mix_u64(hash, gid);
    }
    return hash;
}

[[nodiscard]] EncodedFlatFolderHeader read_header(const MetadataBatchBuffer& buffer) {
    if (buffer.bytes_used < sizeof(EncodedFlatFolderHeader)) {
        throw std::runtime_error("flat folder buffer header is truncated");
    }
    EncodedFlatFolderHeader header {};
    std::memcpy(&header, buffer.bytes.data(), sizeof(header));
    if (header.magic != kFlatFolderMagic || header.version != kFlatFolderVersion) {
        throw std::runtime_error("flat folder buffer header is invalid");
    }
    const std::size_t variable_bytes =
        static_cast<std::size_t>(header.folder_path_bytes) + static_cast<std::size_t>(header.error_bytes);
    if (variable_bytes > buffer.bytes_used - sizeof(EncodedFlatFolderHeader)) {
        throw std::runtime_error("flat folder buffer variable header bytes are truncated");
    }
    return header;
}

void write_header(MetadataBatchBuffer& buffer, const EncodedFlatFolderHeader& header) {
    std::memcpy(buffer.bytes.data(), &header, sizeof(header));
}

[[nodiscard]] std::size_t child_offset(const EncodedFlatFolderHeader& header) {
    return sizeof(EncodedFlatFolderHeader) +
           static_cast<std::size_t>(header.folder_path_bytes) +
           static_cast<std::size_t>(header.error_bytes);
}

bool append_child(MetadataBatchBuffer& buffer,
                  std::uint32_t kind,
                  std::string_view name,
                  std::uint64_t size,
                  std::uint64_t mtime,
                  std::uint32_t mode,
                  std::uint32_t uid,
                  std::uint32_t gid,
                  std::uint64_t flat_file_count,
                  std::uint64_t flat_logical_size_bytes,
                  std::string_view compare_mode) {
    if (name.empty() || name.size() > std::numeric_limits<std::uint32_t>::max()) {
        return false;
    }
    const std::size_t needed = sizeof(EncodedFlatFolderChild) + name.size();
    if (needed > kMetadataBatchBufferBytes - buffer.bytes_used) {
        return false;
    }

    EncodedFlatFolderChild child {};
    child.kind = kind;
    child.name_bytes = static_cast<std::uint32_t>(name.size());
    child.name_hash = child_name_hash(name);
    child.metadata_hash = child_metadata_hash(name,
                                              kind == kFlatFolderChildFile,
                                              size,
                                              mtime,
                                              mode,
                                              uid,
                                              gid,
                                              compare_mode);
    child.logical_size = size;
    child.mtime = mtime;
    child.mode = mode;
    child.uid = uid;
    child.gid = gid;
    child.flat_file_count = flat_file_count;
    child.flat_logical_size_bytes = flat_logical_size_bytes;

    auto* cursor = buffer.bytes.data() + buffer.bytes_used;
    std::memcpy(cursor, &child, sizeof(child));
    cursor += sizeof(child);
    std::memcpy(cursor, name.data(), name.size());
    buffer.bytes_used += static_cast<std::uint32_t>(needed);
    ++buffer.record_count;

    EncodedFlatFolderHeader header = read_header(buffer);
    ++header.child_record_count;
    write_header(buffer, header);
    return true;
}

}  // namespace

bool is_flat_folder_buffer(const MetadataBatchBuffer& buffer) {
    if (buffer.bytes_used < sizeof(std::uint32_t)) {
        return false;
    }
    std::uint32_t magic = 0;
    std::memcpy(&magic, buffer.bytes.data(), sizeof(magic));
    return magic == kFlatFolderMagic;
}

FlatFolderBufferInfo flat_folder_buffer_info(const MetadataBatchBuffer& buffer) {
    const EncodedFlatFolderHeader header = read_header(buffer);
    const auto* path = reinterpret_cast<const char*>(buffer.bytes.data() + sizeof(EncodedFlatFolderHeader));
    const auto* error = path + header.folder_path_bytes;
    FlatFolderBufferInfo info;
    info.folder_path = std::string_view(path, header.folder_path_bytes);
    info.error = std::string_view(error, header.error_bytes);
    info.sequence = header.sequence;
    info.child_record_count = header.child_record_count;
    info.folder_hash = header.folder_hash;
    info.folder_mtime = header.folder_mtime;
    info.folder_mode = header.folder_mode;
    info.folder_uid = header.folder_uid;
    info.folder_gid = header.folder_gid;
    info.scan_started_unix_ns = header.scan_started_unix_ns;
    info.scan_finished_unix_ns = header.scan_finished_unix_ns;
    info.total_file_count = header.total_file_count;
    info.total_folder_count = header.total_folder_count;
    info.total_logical_size_bytes = header.total_logical_size_bytes;
    info.metadata_hash = header.metadata_hash;
    info.final_batch = (header.flags & kFlatFolderFinal) != 0U;
    info.failed = (header.flags & kFlatFolderFailed) != 0U;
    return info;
}

std::size_t flat_folder_transport_size(const MetadataBatchBuffer& buffer) {
    return sizeof(std::uint32_t) + sizeof(std::uint32_t) + buffer.bytes_used;
}

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
                              std::uint64_t scan_finished_unix_ns) {
    const std::string normalized = normalize_path(folder.rel_path);
    if (normalized.size() > std::numeric_limits<std::uint32_t>::max() ||
        error.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::runtime_error("flat folder buffer path or error is too large");
    }
    const std::size_t header_bytes = sizeof(EncodedFlatFolderHeader) + normalized.size() + error.size();
    if (header_bytes > kMetadataBatchBufferBytes) {
        throw std::runtime_error("flat folder buffer header exceeds buffer capacity");
    }

    buffer.bytes_used = 0;
    buffer.record_count = 0;

    EncodedFlatFolderHeader header {};
    header.magic = kFlatFolderMagic;
    header.version = kFlatFolderVersion;
    header.flags = static_cast<std::uint16_t>((final_batch ? kFlatFolderFinal : 0U) |
                                              (failed ? kFlatFolderFailed : 0U));
    header.sequence = sequence;
    header.folder_path_bytes = static_cast<std::uint32_t>(normalized.size());
    header.error_bytes = static_cast<std::uint32_t>(error.size());
    header.folder_hash = folder_hash_for_path(normalized);
    header.folder_mtime = folder.mtime;
    header.folder_mode = folder.mode;
    header.folder_uid = folder.uid;
    header.folder_gid = folder.gid;
    header.scan_started_unix_ns = scan_started_unix_ns;
    header.scan_finished_unix_ns = scan_finished_unix_ns;
    header.total_file_count = total_file_count;
    header.total_folder_count = total_folder_count;
    header.total_logical_size_bytes = total_logical_size_bytes;
    header.metadata_hash = metadata_hash;
    write_header(buffer, header);
    std::memcpy(buffer.bytes.data() + sizeof(header), normalized.data(), normalized.size());
    std::memcpy(buffer.bytes.data() + sizeof(header) + normalized.size(), error.data(), error.size());
    buffer.bytes_used = static_cast<std::uint32_t>(header_bytes);
}

void set_flat_folder_buffer_final(MetadataBatchBuffer& buffer, bool final_batch) {
    EncodedFlatFolderHeader header = read_header(buffer);
    if (final_batch) {
        header.flags |= kFlatFolderFinal;
    } else {
        header.flags &= static_cast<std::uint16_t>(~kFlatFolderFinal);
    }
    write_header(buffer, header);
}

bool append_flat_folder_file(MetadataBatchBuffer& buffer,
                             const FileSpec& file,
                             std::string_view compare_mode) {
    const std::string_view name = child_name_view(file.rel_path);
    return append_child(buffer,
                        kFlatFolderChildFile,
                        name,
                        logical_size(file),
                        file.mtime,
                        file.mode,
                        file.uid,
                        file.gid,
                        0,
                        0,
                        compare_mode);
}

bool append_flat_folder_folder(MetadataBatchBuffer& buffer,
                               const FileSpec& folder,
                               std::string_view compare_mode) {
    const std::string_view name = child_name_view(folder.rel_path);
    return append_child(buffer,
                        kFlatFolderChildFolder,
                        name,
                        0,
                        folder.mtime,
                        folder.mode,
                        folder.uid,
                        folder.gid,
                        0,
                        0,
                        compare_mode);
}

void visit_flat_folder_children(const MetadataBatchBuffer& buffer,
                                const std::function<void(FlatFolderChildView)>& visitor) {
    const EncodedFlatFolderHeader header = read_header(buffer);
    std::size_t offset = child_offset(header);
    std::uint32_t seen = 0;
    while (offset < buffer.bytes_used) {
        if (buffer.bytes_used - offset < sizeof(EncodedFlatFolderChild)) {
            throw std::runtime_error("flat folder child header is truncated");
        }
        EncodedFlatFolderChild child {};
        std::memcpy(&child, buffer.bytes.data() + offset, sizeof(child));
        offset += sizeof(child);
        if (child.name_bytes > buffer.bytes_used - offset) {
            throw std::runtime_error("flat folder child name is truncated");
        }
        const auto* name = reinterpret_cast<const char*>(buffer.bytes.data() + offset);
        FlatFolderChildView view;
        view.name = std::string_view(name, child.name_bytes);
        view.name_hash = child.name_hash;
        view.metadata_hash = child.metadata_hash;
        view.logical_size = child.logical_size;
        view.mtime = child.mtime;
        view.mode = child.mode;
        view.uid = child.uid;
        view.gid = child.gid;
        view.flat_file_count = child.flat_file_count;
        view.flat_logical_size_bytes = child.flat_logical_size_bytes;
        view.is_file = child.kind == kFlatFolderChildFile;
        visitor(view);
        offset += child.name_bytes;
        ++seen;
    }
    if (seen != header.child_record_count || seen != buffer.record_count) {
        throw std::runtime_error("flat folder child count mismatch");
    }
}

std::uint64_t flat_folder_metadata_hash(const std::vector<FileSpec>& files,
                                        const std::vector<FileSpec>& folders,
                                        std::string_view compare_mode) {
    std::uint64_t xor_hash = 0;
    std::uint64_t sum_hash = 0;
    std::uint64_t logical = 0;
    for (const FileSpec& file : files) {
        const std::string_view name = child_name_view(file.rel_path);
        const std::uint64_t size = logical_size(file);
        const std::uint64_t record_hash = child_metadata_hash(name,
                                                              true,
                                                              size,
                                                              file.mtime,
                                                              file.mode,
                                                              file.uid,
                                                              file.gid,
                                                              compare_mode);
        xor_hash ^= record_hash;
        sum_hash += record_hash * kMixPrime;
        logical += size;
    }
    for (const FileSpec& folder : folders) {
        const std::string_view name = child_name_view(folder.rel_path);
        const std::uint64_t record_hash = child_metadata_hash(name,
                                                              false,
                                                              0,
                                                              folder.mtime,
                                                              folder.mode,
                                                              folder.uid,
                                                              folder.gid,
                                                              compare_mode);
        xor_hash ^= record_hash;
        sum_hash += record_hash * kMixPrime;
    }

    std::uint64_t hash = mix_u64(0x4859464c41544648ULL, files.size());
    hash = mix_u64(hash, folders.size());
    hash = mix_u64(hash, logical);
    hash = mix_u64(hash, xor_hash);
    hash = mix_u64(hash, sum_hash);
    return hash;
}

std::uint64_t flat_folder_logical_size(const std::vector<FileSpec>& files) {
    std::uint64_t total = 0;
    for (const FileSpec& file : files) {
        total += logical_size(file);
    }
    return total;
}

}  // namespace hypersync
