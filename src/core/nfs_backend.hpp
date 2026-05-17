#ifndef HYPERSYNC_CORE_NFS_BACKEND_HPP
#define HYPERSYNC_CORE_NFS_BACKEND_HPP

#include <cstddef>
#include <cstdint>
#include <array>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "common/buffer_pool.hpp"
#include "common/slot_pool.hpp"
#include "common/types.hpp"

namespace hypersync {

constexpr std::size_t kNfsEndpointAny = std::numeric_limits<std::size_t>::max();

bool is_nfs_url(std::string_view path);
bool libnfs_support_enabled();
[[nodiscard]] std::vector<std::string> expand_nfs_url_server_candidates(std::string_view root_url);

struct FlatFolderScanBatch {
    FileSpec folder;
    std::vector<FileSpec> files;
    std::vector<FileSpec> directories;
    std::uint64_t scan_started_unix_ns = 0;
    std::uint64_t scan_finished_unix_ns = 0;
    std::uint64_t readdirplus_page_count = 0;
    std::uint64_t readdirplus_page_entries = 0;
    std::uint64_t readdirplus_page_requested_bytes = 0;
    std::uint64_t readdirplus_page_latency_ns = 0;
    std::uint64_t readdirplus_page_max_latency_ns = 0;
    std::uint64_t readdirplus_decode_latency_ns = 0;
    std::uint64_t readdirplus_decode_max_latency_ns = 0;
    bool complete = true;
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

struct RawSmallFileRead {
    FileSpec file;
    BufferHandle handle;
    std::uint64_t bytes_read = 0;
};

struct PackedSmallFilesReadStats {
    std::uint64_t files_read = 0;
    std::uint64_t files_failed = 0;
    std::uint64_t buffers_published = 0;
    std::uint64_t bytes_read = 0;
};

struct NfsAsyncReadLatencySnapshot {
    std::uint64_t queued = 0;
    std::uint64_t completed = 0;
    std::uint64_t failed = 0;
    std::uint64_t zero_reads = 0;
    std::uint64_t short_reads = 0;
    std::uint64_t bytes_requested = 0;
    std::uint64_t bytes_completed = 0;
    std::uint64_t latency_ns = 0;
    std::uint64_t max_latency_ns = 0;
    std::array<std::uint64_t, 8> latency_buckets {};
};

struct NfsAsyncCommandLatencySnapshot {
    std::uint64_t open_completed = 0;
    std::uint64_t open_failed = 0;
    std::uint64_t open_latency_ns = 0;
    std::uint64_t open_max_latency_ns = 0;
    std::uint64_t close_completed = 0;
    std::uint64_t close_failed = 0;
    std::uint64_t close_latency_ns = 0;
    std::uint64_t close_max_latency_ns = 0;
};

struct NfsReaddirplusPageSnapshot {
    std::uint64_t pages = 0;
    std::uint64_t failed_pages = 0;
    std::uint64_t entries = 0;
    std::uint64_t files = 0;
    std::uint64_t directories = 0;
    std::uint64_t requested_bytes = 0;
    std::uint64_t page_latency_ns = 0;
    std::uint64_t max_page_latency_ns = 0;
    std::uint64_t decode_latency_ns = 0;
    std::uint64_t max_decode_latency_ns = 0;
};

void reset_nfs_async_read_latency_metrics();
[[nodiscard]] NfsAsyncReadLatencySnapshot snapshot_nfs_async_read_latency_metrics();
[[nodiscard]] NfsAsyncCommandLatencySnapshot snapshot_nfs_async_command_latency_metrics();
[[nodiscard]] NfsReaddirplusPageSnapshot snapshot_nfs_readdirplus_page_metrics();

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
    virtual void scan_flat_folders_streaming(
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
    [[nodiscard]] virtual std::uint64_t read_file_into(std::string_view rel_path,
                                                       std::uint64_t declared_size,
                                                       std::byte* destination,
                                                       std::size_t destination_bytes) const;
    virtual void open_close_file(std::string_view rel_path) const;
    [[nodiscard]] virtual PackedSmallFilesReadStats read_small_files_packed(
        const std::function<std::optional<FileSpec>()>& file_provider,
        RawBufferPool& pool,
        std::size_t max_in_flight_files,
        const std::function<void(BufferHandle, std::uint64_t, std::uint64_t)>& buffer_visitor,
        const std::function<bool()>& should_stop = {}) const;
    [[nodiscard]] virtual PackedSmallFilesReadStats read_small_files_raw_window(
        const std::function<std::optional<FileSpec>()>& file_provider,
        RawBufferPool& pool,
        std::size_t max_in_flight_files,
        const std::function<void(RawSmallFileRead&&)>& file_visitor,
        const std::function<bool()>& should_stop = {}) const;
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
    [[nodiscard]] virtual std::uint64_t read_file_raw_chunks_by_handle(
        const FileSpec& file,
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

[[nodiscard]] std::unique_ptr<NfsBackend> make_nfs_backend(
    std::string root,
    std::size_t endpoint_index = kNfsEndpointAny,
    std::size_t readdirplus_page_bytes = 0);
[[nodiscard]] std::unique_ptr<TargetWriterBackend> make_target_writer_backend(std::string root);

}  // namespace hypersync

#endif
