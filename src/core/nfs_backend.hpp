#ifndef HYPERSYNC_CORE_NFS_BACKEND_HPP
#define HYPERSYNC_CORE_NFS_BACKEND_HPP

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "common/buffer_pool.hpp"
#include "common/slot_pool.hpp"
#include "common/types.hpp"

namespace hypersync {

bool is_nfs_url(std::string_view path);
bool libnfs_support_enabled();
[[nodiscard]] std::vector<std::string> expand_nfs_url_server_candidates(std::string_view root_url);

struct FlatFolderScanBatch {
    FileSpec folder;
    std::vector<FileSpec> files;
    std::vector<FileSpec> directories;
    std::uint64_t scan_started_unix_ns = 0;
    std::uint64_t scan_finished_unix_ns = 0;
    bool failed = false;
    std::string error;
};

struct OwnedFileChunk {
    std::uint64_t offset = 0;
    std::string data;
};

struct PooledFileChunk {
    std::uint64_t offset = 0;
    DataSlotHandle handle;
};

struct RawFileChunk {
    std::uint64_t offset = 0;
    BufferHandle handle;
};

class NfsBackend {
public:
    virtual ~NfsBackend() = default;

    [[nodiscard]] virtual std::vector<FileSpec> list_files(bool recursive) const = 0;
    [[nodiscard]] virtual std::vector<FileSpec> list_directories(bool recursive) const = 0;
    virtual void visit_files(bool recursive, const std::function<void(FileSpec)>& visitor) const;
    virtual void visit_metadata(bool recursive,
                                const std::function<void(FileSpec)>& file_visitor,
                                const std::function<void(FileSpec)>& directory_visitor) const;
    virtual void visit_metadata_at(std::string_view rel_path,
                                   bool recursive,
                                   const std::function<void(FileSpec)>& file_visitor,
                                   const std::function<void(FileSpec)>& directory_visitor) const;
    virtual void visit_folder(std::string_view rel_path,
                              const std::function<void(FileSpec)>& file_visitor,
                              const std::function<void(FileSpec)>& directory_visitor) const;
    virtual void scan_flat_folders(
        std::size_t outstanding_folders,
        const std::function<std::optional<FileSpec>(bool wait_for_work)>& folder_provider,
        const std::function<bool()>& should_stop,
        const std::function<void(FlatFolderScanBatch)>& folder_visitor) const;
    [[nodiscard]] virtual FileSpec load_file(std::string_view rel_path) const = 0;
    [[nodiscard]] virtual FileSpec load_file(std::string_view rel_path, std::size_t outstanding_requests) const;
    [[nodiscard]] virtual std::uint64_t read_file_discard(std::string_view rel_path,
                                                          std::uint64_t declared_size,
                                                          std::size_t outstanding_requests) const;
    [[nodiscard]] virtual std::uint64_t read_file_discard(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        const std::function<void(std::uint64_t)>& bytes_visitor) const;
    [[nodiscard]] virtual std::uint64_t read_file_stream(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        const std::function<void(std::string_view)>& data_visitor) const;
    [[nodiscard]] virtual std::uint64_t read_file_owned_chunks(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        const std::function<void(OwnedFileChunk&&)>& data_visitor) const;
    [[nodiscard]] virtual std::uint64_t read_file_pooled_chunks(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        DataSlotPool& pool,
        const std::function<void(PooledFileChunk&&)>& data_visitor) const;
    [[nodiscard]] virtual std::uint64_t read_file_raw_chunks(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        RawBufferPool& pool,
        const std::function<void(RawFileChunk&&)>& data_visitor,
        const std::function<bool()>& should_stop = {},
        bool copy_payload_to_buffer = true) const;
    [[nodiscard]] virtual std::uint64_t visit_file_chunks(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        const std::function<void(std::uint64_t offset, std::string_view data)>& data_visitor) const;
    [[nodiscard]] virtual std::optional<FileSpec> stat_path(std::string_view rel_path) const = 0;
    [[nodiscard]] virtual bool metadata_matches(std::string_view rel_path,
                                                std::uint64_t size,
                                                std::uint64_t mtime) const = 0;
    [[nodiscard]] virtual std::uint64_t file_hash(std::string_view rel_path) const = 0;
    [[nodiscard]] virtual bool uses_async_api() const = 0;
    [[nodiscard]] virtual std::string description() const = 0;
};

class TargetWriterBackend {
public:
    virtual ~TargetWriterBackend() = default;

    virtual void ensure_directory(const FileSpec& spec) = 0;
    virtual void apply_directory_metadata(const FileSpec& spec) = 0;
    virtual void write_chunk(const FileSpec& spec, std::string_view data, std::uint64_t offset) = 0;
    virtual void finish_file(const FileSpec& spec) = 0;
    virtual void abort_file(std::string_view rel_path) noexcept = 0;
    [[nodiscard]] virtual std::uint64_t file_hash(std::string_view rel_path) const = 0;
    [[nodiscard]] virtual bool uses_async_api() const = 0;
};

[[nodiscard]] std::unique_ptr<NfsBackend> make_nfs_backend(std::string root);
[[nodiscard]] std::unique_ptr<TargetWriterBackend> make_target_writer_backend(std::string root);

}  // namespace hypersync

#endif
