#ifndef HYPERSYNC_JOBS_NFS_DATA_READER_HPP
#define HYPERSYNC_JOBS_NFS_DATA_READER_HPP

#include <memory>
#include <functional>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "common/buffer_pool.hpp"
#include "common/slot_pool.hpp"
#include "common/types.hpp"
#include "jobs/queue_job.hpp"

namespace hypersync {

class ConfigStore;
class NfsBackend;
class RawBufferPool;
struct OwnedFileChunk;
struct PooledFileChunk;
struct PackedSmallFilesReadStats;
struct RawFileChunk;
struct RawSmallFileRead;

struct NfsDataReaderConfig {
    std::size_t data_reader_worker_count;
    std::size_t outstanding_requests;
    std::size_t small_file_async_window;
    std::size_t large_file_parallelism;
    std::size_t small_file_threshold;
    std::size_t large_chunk_bytes;
    double pause_large_pool_percent;
    double resume_large_pool_percent;
    std::string source_root;
    bool copy_data_from_nfs;
    bool pack_small_files;
    std::size_t endpoint_index;

    NfsDataReaderConfig();
    NfsDataReaderConfig(std::size_t data_reader_worker_count,
                        std::size_t outstanding_requests,
                        std::size_t small_file_async_window,
                        std::size_t large_file_parallelism,
                        std::size_t small_file_threshold,
                        std::size_t large_chunk_bytes,
                        double pause_large_pool_percent,
                        double resume_large_pool_percent,
                        std::string source_root,
                        bool copy_data_from_nfs = true,
                        bool pack_small_files = false,
                        std::size_t endpoint_index = std::numeric_limits<std::size_t>::max());
};

[[nodiscard]] NfsDataReaderConfig load_nfs_data_reader_config(const ConfigStore& config);

class NfsDataReader : public TypedQueueJob<DataChunk> {
public:
    explicit NfsDataReader(NfsDataReaderConfig config = {});
    ~NfsDataReader();

    [[nodiscard]] std::vector<DataChunk> chunk_file(const FileSpec& file) const;
    [[nodiscard]] FileSpec load_file(std::string_view rel_path) const;
    [[nodiscard]] std::uint64_t read_file_bytes(const FileSpec& file) const;
    [[nodiscard]] std::uint64_t read_file_bytes(
        const FileSpec& file,
        const std::function<void(std::uint64_t)>& bytes_visitor) const;
    [[nodiscard]] std::uint64_t stream_file_data(
        const FileSpec& file,
        const std::function<void(std::string_view)>& data_visitor) const;
    [[nodiscard]] std::uint64_t read_file_into(const FileSpec& file,
                                               std::byte* destination,
                                               std::size_t destination_bytes) const;
    void open_close_file(const FileSpec& file) const;
    [[nodiscard]] PackedSmallFilesReadStats read_small_files_packed(
        const std::function<std::optional<FileSpec>()>& file_provider,
        RawBufferPool& pool,
        const std::function<void(BufferHandle, std::uint64_t, std::uint64_t)>& buffer_visitor,
        const std::function<bool()>& should_stop = {}) const;
    [[nodiscard]] PackedSmallFilesReadStats read_small_files_raw_window(
        const std::function<std::optional<FileSpec>()>& file_provider,
        RawBufferPool& pool,
        const std::function<void(RawSmallFileRead&&)>& file_visitor,
        const std::function<bool()>& should_stop = {}) const;
    [[nodiscard]] std::uint64_t stream_file_owned_chunks(
        const FileSpec& file,
        const std::function<void(OwnedFileChunk&&)>& data_visitor) const;
    [[nodiscard]] std::uint64_t stream_file_pooled_chunks(
        const FileSpec& file,
        DataSlotPool& pool,
        const std::function<void(PooledFileChunk&&)>& data_visitor) const;
    [[nodiscard]] std::uint64_t stream_file_raw_chunks(
        const FileSpec& file,
        RawBufferPool& pool,
        const std::function<void(RawFileChunk&&)>& data_visitor,
        const std::function<bool()>& should_stop = {}) const;
    [[nodiscard]] std::uint64_t visit_file_chunks(
        const FileSpec& file,
        const std::function<void(std::uint64_t offset, std::string_view data)>& data_visitor) const;
    [[nodiscard]] std::vector<DataChunk> read_file(std::string_view rel_path) const;
    void publish_file(const FileSpec& file);
    void publish_path(std::string_view rel_path);
    [[nodiscard]] bool should_pause(double large_pool_usage_percent) const;
    [[nodiscard]] bool should_resume(double large_pool_usage_percent) const;
    [[nodiscard]] bool using_async_backend() const;
    [[nodiscard]] const NfsDataReaderConfig& config() const;

private:
    [[nodiscard]] NfsBackend& backend() const;

    NfsDataReaderConfig config_;
    mutable std::unique_ptr<NfsBackend> backend_;
};

}  // namespace hypersync

#endif
